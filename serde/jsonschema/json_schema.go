/**
 * Copyright 2022 Confluent Inc.
 * Copyright 2022-2023 Jerome Bidault (jeromedbtdev@gmail.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file has been modified by Jerome Bidault (jeromebdtdev@gmail.com) to include additional functionality.
 */

package jsonschema

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"

	schemaregistry "github.com/djedjethai/kfk-schemaregistry"
	"github.com/djedjethai/kfk-schemaregistry/serde"
	"github.com/invopop/jsonschema"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"
)

// Serializer represents a JSON Schema serializer
type Serializer struct {
	serde.BaseSerializer
	validate bool
}

// Deserializer represents a JSON Schema deserializer
type Deserializer struct {
	serde.BaseDeserializer
	validate bool
}

var _ serde.Serializer = new(Serializer)
var _ serde.Deserializer = new(Deserializer)

// NewSerializer creates a JSON serializer for generic objects
func NewSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*Serializer, error) {
	s := &Serializer{
		validate: conf.EnableValidation,
	}
	err := s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of generic data to JSON
func (s *Serializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	return s.helperRunSerialize(topic, msg)
}

func (s Serializer) addFieldToStructInterface(someStruct interface{}, fullyQualifiedName string) interface{} {
	// Use reflection to create a new instance of the struct
	v := reflect.ValueOf(someStruct)
	t := v.Type()
	if t.Kind() != reflect.Struct {
		log.Println("Input is not a struct")
	}

	// Function to recursively add fields from embedded structs
	var addEmbeddedFields func(fields []reflect.StructField, typ reflect.Type) []reflect.StructField
	addEmbeddedFields = func(fields []reflect.StructField, typ reflect.Type) []reflect.StructField {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Anonymous && field.Type.Kind() == reflect.Struct {
				fields = addEmbeddedFields(fields, field.Type)
			} else {
				fields = append(fields, field)
			}
		}
		return fields
	}

	// Create a slice to hold the new struct field list
	newFields := addEmbeddedFields(nil, t)

	// Add the new field
	newFields = append(newFields, reflect.StructField{
		Name: "FullyQualifiedName",
		Type: reflect.TypeOf(""),
		Tag:  reflect.StructTag("json:\"fullyQualifiedName\""),
	})

	// Create a new type (struct) with the additional field and embedded fields
	newType := reflect.StructOf(newFields)

	// Create an instance of the new type
	newStruct := reflect.New(newType).Elem()

	// Copy existing fields, including embedded fields
	for i := 0; i < t.NumField(); i++ {
		field := v.Field(i)
		newField := newStruct.Field(i)
		newField.Set(field)
	}

	// Add the new field
	newStruct.FieldByName("FullyQualifiedName").SetString(fullyQualifiedName)

	return newStruct.Interface()
}

func (s *Serializer) helperRunSerialize(subject string, msg interface{}) ([]byte, error) {
	jschema := jsonschema.Reflect(msg)

	raw, err := json.Marshal(jschema)
	if err != nil {
		return nil, err
	}

	info := schemaregistry.SchemaInfo{
		Schema:     string(raw),
		SchemaType: "JSON",
	}

	id, err := s.GetID(subject, msg, info)
	if err != nil {
		return nil, err
	}
	raw, err = json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(raw, &obj)
		if err != nil {
			return nil, err
		}
		jschema, err := toJSONSchema(s.Client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	payload, err := s.WriteBytes(id, raw)
	if err != nil {
		return nil, err
	}
	return payload, nil

}

// SerializeRecordName implements serialization of generic data to JSON
func (s *Serializer) SerializeRecordName(subject string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	// get the fully qualified name
	msgFQN := reflect.TypeOf(msg).String()

	// test add field
	msg = s.addFieldToStructInterface(msg, msgFQN)

	return s.helperRunSerialize(msgFQN, msg)
}

// NewDeserializer creates a JSON deserializer for generic objects
func NewDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*Deserializer, error) {
	s := &Deserializer{
		validate: conf.EnableValidation,
	}
	err := s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of generic data from JSON
func (s *Deserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}

	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[5:], &obj)
		if err != nil {
			return nil, err
		}
		jschema, err := toJSONSchema(s.Client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}

	msg, err := s.MessageFactory(subject, "")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(payload[5:], msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *Deserializer) deserializeStringField(bytes []byte, fieldName string) (string, error) {
	var fieldNameBytes []byte
	var fieldValueBytes []byte
	fieldNameLen := 0
	readingFieldName := true

	for _, b := range bytes {
		if readingFieldName {
			if fieldNameLen == 0 {
				// The first byte of the field name indicates its length
				fieldNameLen = int(b)
			} else {
				// Accumulate bytes for the field name
				fieldNameBytes = append(fieldNameBytes, b)
				if len(fieldNameBytes) == fieldNameLen {
					readingFieldName = false
				}
			}
		} else {
			// Accumulate bytes for the field value
			fieldValueBytes = append(fieldValueBytes, b)
		}
	}

	if fieldName != string(fieldNameBytes) {
		return "", fmt.Errorf("field not found: %s", fieldName)
	}

	return string(fieldValueBytes), nil
}

// DeserializeRecordName deserialise bytes
func (s *Deserializer) DeserializeRecordName(subjects map[string]interface{}, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}

	type Payload struct {
		FullyQualifiedName string `json:"fullyQualifiedName"`
	}

	// get the real payload
	pl := payload[5:]

	// Create a variable to hold the extracted value
	var tmp Payload

	// Unmarshal the JSON string into the struct
	err := json.Unmarshal(pl, &tmp)
	if err != nil {
		fmt.Println("Error:", err)
		// return
	}

	// Access the extracted value
	fullyQualifiedName := tmp.FullyQualifiedName

	// make sure the incomming event own the right fullyQualifiedName
	if _, ok := subjects[fullyQualifiedName]; !ok {
		return nil, fmt.Errorf("Non matching subject")
	}

	info, err := s.GetSchema(fullyQualifiedName, payload)
	// info, err := s.GetSchema("", payload)
	if err != nil {
		return nil, err
	}

	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[5:], &obj)
		if err != nil {
			return nil, err
		}
		jschema, err := toJSONSchema(s.Client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	subject, err := s.SubjectNameStrategy(fullyQualifiedName, s.SerdeType, info)
	if err != nil {
		return nil, err
	}

	msg, err := s.MessageFactory(subject, "")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(payload[5:], msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// DeserializeIntoRecordName deserialize bytes into the map interface{}
func (s *Deserializer) DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error {
	_, err := s.DeserializeRecordName(subjects, payload)

	return err
}

// DeserializeInto implements deserialization of generic data from JSON to the given object
func (s *Deserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return err
	}
	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[5:], &obj)
		if err != nil {
			return err
		}
		jschema, err := toJSONSchema(s.Client, info)
		if err != nil {
			return err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return err
		}
	}
	err = json.Unmarshal(payload[5:], msg)
	if err != nil {
		return err
	}
	return nil
}

func toJSONSchema(c schemaregistry.Client, schema schemaregistry.SchemaInfo) (*jsonschema2.Schema, error) {
	deps := make(map[string]string)
	err := serde.ResolveReferences(c, schema, deps)
	if err != nil {
		return nil, err
	}
	compiler := jsonschema2.NewCompiler()
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(deps[url])), nil
	}
	url := "schema.json"
	if err := compiler.AddResource(url, strings.NewReader(schema.Schema)); err != nil {
		return nil, err
	}
	return compiler.Compile(url)
}
