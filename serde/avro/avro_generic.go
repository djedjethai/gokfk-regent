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

package avro

import (
	// "fmt"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"unsafe"

	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
	schemaregistry "github.com/djedjethai/kfk-schemaregistry"
	"github.com/djedjethai/kfk-schemaregistry/serde"
	"github.com/heetch/avro"
)

// GenericSerializer represents a generic Avro serializer
type GenericSerializer struct {
	serde.BaseSerializer
}

// GenericDeserializer represents a generic Avro deserializer
type GenericDeserializer struct {
	serde.BaseDeserializer
}

var _ serde.Serializer = new(GenericSerializer)
var _ serde.Deserializer = new(GenericDeserializer)

// NewGenericSerializer creates an Avro serializer for generic objects
func NewGenericSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*GenericSerializer, error) {
	s := &GenericSerializer{}
	err := s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s GenericSerializer) addFieldToStructInterface(someStruct interface{}, fullyQualifiedName string) interface{} {
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
		Tag:  reflect.StructTag("avro:\"fullyQualifiedName\""),
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

// Serialize implements serialization of generic Avro data
func (s *GenericSerializer) SerializeRecordName(subject string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	msgFQN := reflect.TypeOf(msg).String()
	msgFQN = strings.TrimLeft(msgFQN, "*")
	log.Println("avro_generic.go - SerializeRecordName - see msgFQN: ", msgFQN)

	// test add field
	// msg = s.addFieldToStructInterface(msg, msgFQN)

	val := reflect.ValueOf(msg)
	if val.Kind() == reflect.Ptr {
		// avro.TypeOf expects an interface containing a non-pointer
		msg = val.Elem().Interface()
	}
	avroType, err := avro.TypeOf(msg)
	if err != nil {
		return nil, err
	}

	// change the name......................
	// Unmarshal the JSON into a map
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(avroType.String()), &data); err != nil {
		log.Println("Error unmarshaling JSON:", err)
	}

	// Modify the "name" field to a new value
	data["name"] = msgFQN

	// Marshal the modified data back to a JSON string
	modifiedJSON, err := json.Marshal(data)
	if err != nil {
		log.Println("Error marshaling JSON:", err)
	}

	log.Println("avro_generic.go - SerializeRecordName - modifiedJason: ", string(modifiedJSON))
	// end change the name.............................

	// log.Println("avro_generic.go - Serialize - msgFQN: ", msgFQN)
	// log.Println("avro_generic.go - Serialize - avroType: ", avroType)
	// log.Println("avro_generic.go - Serialize - avroTypeName: ", avroType.Name())
	// log.Println("avro_generic.go - Serialize - avroTypeString: ", avroType.String())

	info := schemaregistry.SchemaInfo{
		Schema: string(modifiedJSON),
	}

	log.Println("avro_generic.go - SerializeRecordName - info to save in sr: ", info)

	id, err := s.GetID(msgFQN, msg, info)
	if err != nil {
		return nil, err
	}
	msgBytes, _, err := avro.Marshal(msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, msgBytes)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// Serialize implements serialization of generic Avro data
func (s *GenericSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	val := reflect.ValueOf(msg)
	if val.Kind() == reflect.Ptr {
		// avro.TypeOf expects an interface containing a non-pointer
		msg = val.Elem().Interface()
	}
	avroType, err := avro.TypeOf(msg)
	if err != nil {
		return nil, err
	}

	info := schemaregistry.SchemaInfo{
		Schema: avroType.String(),
	}
	id, err := s.GetID(topic, msg, info)
	if err != nil {
		return nil, err
	}
	msgBytes, _, err := avro.Marshal(msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, msgBytes)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewGenericDeserializer creates an Avro deserializer for generic objects
func NewGenericDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*GenericDeserializer, error) {
	s := &GenericDeserializer{}
	err := s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *GenericDeserializer) DeserializeRecordName(subjects map[string]interface{}, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}

	info, err := s.GetSchema("", payload)
	if err != nil {
		return nil, err
	}

	// start the hach .............................................
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(info.Schema), &data); err != nil {
		log.Println("Error unmarshaling JSON:", err)
	}

	// Modify the "name" field to a new value
	name := data["name"].(string)
	namespace := data["namespace"].(string)
	fullyQualifiedName := fmt.Sprintf("%s.%s", namespace, name)
	log.Println("avro_generic.go - DeserializeRecordName - name: ", name)
	log.Println("avro_generic.go - DeserializeRecordName - nameSpace: ", namespace)
	log.Println("avro_generic.go - DeserializeRecordName - fullyQualifiedName: ", fullyQualifiedName)

	log.Println("avro_generic.go - Deserialize - infoAfterHach: ", info)
	// {{"type":"record","name":"Person","fields":[{"name":"Name","type":"string","default":""},{"name":"Age","type":"long","default":0}]}  [] }

	if _, ok := subjects[fullyQualifiedName]; !ok {
		return nil, fmt.Errorf("No matching subject")
	}

	writer, name, err := s.toType(info)
	if err != nil {
		return nil, err
	}

	// // TODO change the SubjectNameStrategy
	// subject, err := s.SubjectNameStrategy(fullyQualifiedName, s.SerdeType, info)
	// if err != nil {
	// 	return nil, err
	// }
	msg, err := s.MessageFactory(fullyQualifiedName, name)
	if err != nil {
		return nil, err
	}
	_, err = avro.Unmarshal(payload[5:], msg, writer)
	return msg, err

	// return nil, nil
}

func (s *GenericDeserializer) DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error {
	return nil
}

// Deserialize implements deserialization of generic Avro data
func (s *GenericDeserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}

	log.Println("avro_generic.go - Deserialize - info: ", info)

	writer, name, err := s.toType(info)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}
	msg, err := s.MessageFactory(subject, name)
	if err != nil {
		return nil, err
	}
	_, err = avro.Unmarshal(payload[5:], msg, writer)
	return msg, err
}

// DeserializeInto implements deserialization of generic Avro data to the given object
func (s *GenericDeserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return err
	}
	writer, _, err := s.toType(info)
	_, err = avro.Unmarshal(payload[5:], msg, writer)
	return err
}

func (s *GenericDeserializer) toType(schema schemaregistry.SchemaInfo) (*avro.Type, string, error) {
	t := avro.Type{}
	avroType, err := s.toAvroType(schema)
	if err != nil {
		return nil, "", err
	}

	// Use reflection to set the private avroType field of avro.Type
	setPrivateAvroType(&t, avroType)

	return &t, avroType.Name(), nil
}

func (s *GenericDeserializer) toAvroType(schema schemaregistry.SchemaInfo) (schema.AvroType, error) {
	ns := parser.NewNamespace(false)
	return resolveAvroReferences(s.Client, schema, ns)
}

// From https://stackoverflow.com/questions/42664837/how-to-access-unexported-struct-fields/43918797#43918797
func setPrivateAvroType(t *avro.Type, avroType schema.AvroType) {
	rt := reflect.ValueOf(t).Elem()
	rf := rt.Field(0)
	reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(avroType))
}
