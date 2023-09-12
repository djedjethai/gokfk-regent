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
	// "encoding/binary"
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

// // Serialize implements serialization of generic data to JSON
// func (s *Serializer) Serialize(topic string, msg interface{}) ([]byte, error) {
// 	if msg == nil {
// 		return nil, nil
// 	}
//
// 	jschema := jsonschema.Reflect(msg)
//
// 	raw, err := json.Marshal(jschema)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	log.Println("json_schema.go - Serialize - see jschema: ", string(raw))
//
// 	info := schemaregistry.SchemaInfo{
// 		Schema:     string(raw),
// 		SchemaType: "JSON",
// 	}
// 	id, err := s.GetID(topic, msg, info)
// 	if err != nil {
// 		return nil, err
// 	}
// 	raw, err = json.Marshal(msg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if s.validate {
// 		// Need to unmarshal to pure interface
// 		var obj interface{}
// 		err = json.Unmarshal(raw, &obj)
// 		if err != nil {
// 			return nil, err
// 		}
// 		jschema, err := toJSONSchema(s.Client, info)
// 		if err != nil {
// 			return nil, err
// 		}
// 		err = jschema.Validate(obj)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	payload, err := s.WriteBytes(id, raw)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return payload, nil
// }

func (s Serializer) addFieldToStructInterface(someStruct interface{}, fullyQualifiedName string) interface{} {
	// Use reflection to create a new instance of the struct
	v := reflect.ValueOf(someStruct)
	t := v.Type()
	if t.Kind() != reflect.Struct {
		panic("Input is not a struct")
	}

	// newStruct.FieldByName("FullyQualifiedName").SetString("")
	// newStruct := reflect.New(t).Elem()

	// Create a new type (struct) with the additional field
	newType := reflect.StructOf([]reflect.StructField{
		// Copy the existing fields
		t.Field(0),
		t.Field(1),
		// Add the new field
		{
			Name: "FullyQualifiedName",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag("json:\"fullyQualifiedName\""),
		},
	})

	newStruct := reflect.New(newType).Elem()

	// Copy existing fields
	for i := 0; i < t.NumField(); i++ {
		// log.Println("json_schema.go - addFieldToStructInterface - see firlds: ", i)
		field := v.Field(i)
		newStruct.Field(i).Set(field)
	}

	// log.Println("json_schema.go - addFieldToStructInterface - see fqn: ", fullyQualifiedName)
	// Add the new field
	newStruct.FieldByName("FullyQualifiedName").SetString(fullyQualifiedName)

	return newStruct.Interface()
}

// Serialize implements serialization of generic data to JSON
func (s *Serializer) Serialize(subject string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	// get the fully qualified name
	msgFQN := reflect.TypeOf(msg).String()
	// log.Println("json_schema.go - Serialize - see msgFQN: ", msgFQN)

	// test add field
	msg = s.addFieldToStructInterface(msg, msgFQN)

	jschema := jsonschema.Reflect(msg)

	raw, err := json.Marshal(jschema)
	if err != nil {
		return nil, err
	}

	info := schemaregistry.SchemaInfo{
		Schema:     string(raw),
		SchemaType: "JSON",
		// SchemaFullyQualifiedName: msgFQN,
	}

	log.Println("json_schema.go - Serialize - see jschema: ", info)

	// id, err := s.GetID(subject, msg, info)
	id, err := s.GetID(msgFQN, msg, info)
	log.Println("json_schema.go - Serialize - see jschema: ", id)
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

	// log.Println("json_schema.go - Deserialize - info: ", info)

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

	// log.Println("json_schema.go - Deserialize - subject: ", subject)

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

// func (s *Deserializer) getFieldFromBytes(bytes []byte, fieldName string, element interface{}) {
// 	// v := reflect.New(reflect.TypeOf(element)).Elem() ok if not a pointer
// 	v := reflect.ValueOf(element).Elem()
// 	size := v.NumField()
// 	offset := 0
//
// 	fmt.Println("json_schema.go - getFieldFromBytes")
// 	for i := 0; i < size; i++ {
// 		field := v.Type().Field(i)
// 		fmt.Println("json_schema.go - getFieldFromBytes - field: ", field.Name)
// 		if field.Name == fieldName {
// 			fieldSize := int(field.Type.Size())
// 			switch field.Type.Kind() {
// 			case reflect.String:
// 				fieldValue := string(bytes[offset : offset+fieldSize])
// 				// return fieldValue, nil
// 				fmt.Println("json_schema.go - getFieldFromBytes - the string field: ", fieldValue)
// 			case reflect.Int:
// 				fieldValue := binary.LittleEndian.Uint32(bytes[offset : offset+fieldSize])
// 				// return int(fieldValue), nil
// 				fmt.Println("json_schema.go - getFieldFromBytes - the int field: ", int(fieldValue))
// 			}
// 		}
// 		offset += int(field.Type.Size())
// 	}
// }

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

	fmt.Println("the sssssssssssstttttttrrrrrrrr 000: ", fullyQualifiedName)

	info, err := s.GetSchema(fullyQualifiedName, payload)
	// info, err := s.GetSchema("", payload)
	if err != nil {
		return nil, err
	}

	log.Println("json_schema.go - DeserializeRecordName - info - FQN: ", info)

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

	log.Println("json_schema.go - DeserializeRecordName - subject: ", subject)

	msg, err := s.MessageFactory(subject, "")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(payload[5:], msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
	// }

	// return nil, nil
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

func (s *Deserializer) DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error {
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
