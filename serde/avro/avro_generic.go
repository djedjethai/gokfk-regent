/**
 * Copyright 2022 Confluent Inc.
 * Copyright 2023 Jerome Bidault (jeromedbtdev@gmail.com).
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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
	schemaregistry "github.com/djedjethai/gokfk-regent"
	"github.com/djedjethai/gokfk-regent/serde"
	"github.com/heetch/avro"
	"github.com/linkedin/goavro"
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

func (s *GenericSerializer) addFullyQualifiedNameToSchema(avroStr, msgFQN string) ([]byte, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(avroStr), &data); err != nil {
		return nil, err
	}

	parts := strings.Split(msgFQN, ".")
	if len(parts) > 0 {
		var namespace string
		if len(parts) == 2 {
			namespace = parts[0]
		} else if len(parts) > 2 {
			for i := 0; i < len(parts)-1; i++ {
				if i == 0 {
					namespace += parts[0]
				} else {
					namespace += fmt.Sprintf(".%v", parts[i])
				}
			}

		}
		data["namespace"] = namespace
	}

	return json.Marshal(data)
}

// SerializeTopicRecordName implements serialization of generic Avro data
func (s *GenericSerializer) SerializeTopicRecordName(topic string, msg interface{}, subject ...string) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	msgFQN := reflect.TypeOf(msg).String()
	msgFQN = strings.TrimLeft(msgFQN, "*") // in case

	val := reflect.ValueOf(msg)
	if val.Kind() == reflect.Ptr {
		// avro.TypeOf expects an interface containing a non-pointer
		msg = val.Elem().Interface()
	}
	avroType, err := avro.TypeOf(msg)
	if err != nil {
		return nil, err
	}

	modifiedJSON, err := s.addFullyQualifiedNameToSchema(avroType.String(), msgFQN)
	if err != nil {
		return nil, err
	}

	// add topic to the fullyQualifiedName
	msgFQN = fmt.Sprintf("%s-%s", topic, msgFQN)

	if len(subject) > 0 {
		if msgFQN != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, msgFQN, subject[0])
		}
	}

	info := schemaregistry.SchemaInfo{
		Schema: string(modifiedJSON),
	}

	id, fromSR, err := s.GetID(msgFQN, msg, info)
	if err != nil {
		return nil, err
	}
	msgBytes, _, err := avro.Marshal(msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, fromSR, msgBytes)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// SerializeRecordName implements serialization of generic Avro data
func (s *GenericSerializer) SerializeRecordName(msg interface{}, subject ...string) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	msgFQN := reflect.TypeOf(msg).String()
	msgFQN = strings.TrimLeft(msgFQN, "*") // in case

	if len(subject) > 0 {
		if msgFQN != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, msgFQN, subject[0])
		}
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

	modifiedJSON, err := s.addFullyQualifiedNameToSchema(avroType.String(), msgFQN)
	if err != nil {
		return nil, err
	}

	info := schemaregistry.SchemaInfo{
		Schema: string(modifiedJSON),
	}

	id, fromSR, err := s.GetID(msgFQN, msg, info)
	if err != nil {
		return nil, err
	}
	msgBytes, _, err := avro.Marshal(msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, fromSR, msgBytes)
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
	id, fromSR, err := s.GetID(topic, msg, info)
	if err != nil {
		return nil, err
	}
	msgBytes, _, err := avro.Marshal(msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, fromSR, msgBytes)
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
	s.MessageFactory = s.avroMessageFactory
	return s, nil
}

// DeserializeTopicRecordName implements deserialization of generic Avro data
func (s *GenericDeserializer) DeserializeTopicRecordName(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}

	info, err := s.GetSchema("", payload)
	if err != nil {
		return nil, err
	}

	// recreate the fullyQualifiedName
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(info.Schema), &data); err != nil {
		return nil, err
	}
	name := data["name"].(string)
	namespace := data["namespace"].(string)
	msgFullyQlfName := fmt.Sprintf("%s.%s", namespace, name)

	topicMsgFullyQlfNameValue, err := s.SubjectNameStrategy(topic, s.SerdeType, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	// loop on info.Subject to assert the subject name
	var subjects []string
	for _, v := range info.Subject {
		if string(v) == topicMsgFullyQlfNameValue {
			subjects = append(subjects, v)
			break
		}
	}
	if len(subjects) == 0 {
		// retry with updating the cache
		_, err = s.retryGetSubjects(payload, subjects, topicMsgFullyQlfNameValue)
		if err != nil {
			return nil, err
		}
		if len(subjects) == 0 {
			return nil, fmt.Errorf("no subject found for: %v", topicMsgFullyQlfNameValue)
		}
	}

	msg, err := s.MessageFactory(subjects, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	if msg == struct{}{} {
		tmp, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		info.Schema = string(tmp)

		codec, err := goavro.NewCodec(info.Schema)
		if err != nil {
			return nil, err
		}

		native, _, err := codec.NativeFromBinary(payload[6:])
		if err != nil {
			return nil, err
		}

		return native, nil
	}

	writer, _, err := s.toType(info)
	if err != nil {
		return nil, err
	}
	_, err = avro.Unmarshal(payload[6:], msg, writer)
	return msg, err
}

func (s *GenericDeserializer) retryGetSubjects(payload []byte, subjects []string, topicMFQNValue string) ([]string, error) {
	payload[5] = 1
	infoLast, err := s.GetSchema("", payload)
	if err != nil {
		return nil, err
	}

	for _, s := range infoLast.Subject {
		if topicMFQNValue == s {
			subjects = append(subjects, s)
			break
		}
	}

	return infoLast.Subject, nil
}

// DeserializeRecordName implements deserialization of generic Avro data
func (s *GenericDeserializer) DeserializeRecordName(payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}

	info, err := s.GetSchema("", payload)
	if err != nil {
		return nil, err
	}

	// recreate the fullyQualifiedName
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(info.Schema), &data); err != nil {
		return nil, err
	}
	name := data["name"].(string)
	namespace := data["namespace"].(string)
	msgFullyQlfName := fmt.Sprintf("%s.%s", namespace, name)

	msgFullyQlfNameValue, err := s.SubjectNameStrategy("", s.SerdeType, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	var subjects []string
	for _, s := range info.Subject {
		if s == msgFullyQlfNameValue {
			subjects = append(subjects, s)
			break
		}
	}
	if len(subjects) == 0 {
		// retry with updating the cache
		_, err = s.retryGetSubjects(payload, subjects, msgFullyQlfNameValue)
		if err != nil {
			return nil, err
		}
		if len(subjects) == 0 {
			return nil, fmt.Errorf("no subject found for: %v", msgFullyQlfNameValue)
		}
	}

	msg, err := s.MessageFactory(subjects, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	if msg == struct{}{} {
		codec, err := goavro.NewCodec(info.Schema)
		if err != nil {
			return nil, err
		}

		native, _, err := codec.NativeFromBinary(payload[6:])
		if err != nil {
			return nil, err
		}

		return native, nil
	}

	writer, _, err := s.toType(info)
	if err != nil {
		return nil, err
	}
	_, err = avro.Unmarshal(payload[6:], msg, writer)
	return msg, err
}

// DeserializeIntoTopicRecordName implements deserialization of generic Avro data
func (s *GenericDeserializer) DeserializeIntoTopicRecordName(topic string, subjects map[string]interface{}, payload []byte) error {
	if payload == nil {
		return fmt.Errorf("Empty payload")
	}

	info, err := s.GetSchema("", payload)
	if err != nil {
		return err
	}

	// recreate the fullyQualifiedName
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(info.Schema), &data); err != nil {
		return err
	}
	name := data["name"].(string)
	namespace := data["namespace"].(string)
	msgFullyQlfName := fmt.Sprintf("%s.%s", namespace, name)

	topicMsgFullyQlfNameValue, err := s.SubjectNameStrategy(topic, s.SerdeType, msgFullyQlfName)
	if err != nil {
		return err
	}

	// loop on info.Subject to assert the subject name
	var sub []string
	for _, v := range info.Subject {
		if string(v) == topicMsgFullyQlfNameValue {
			sub = append(sub, v)
			break
		}
	}
	if len(sub) == 0 {
		// retry with updating the cache
		_, err = s.retryGetSubjects(payload, sub, topicMsgFullyQlfNameValue)
		if err != nil {
			return err
		}
		if len(sub) == 0 {
			return fmt.Errorf("no subject found for: %v", topicMsgFullyQlfNameValue)
		}
	}

	v, ok := subjects[sub[0]]
	if !ok {
		return fmt.Errorf("unfound subject declaration")
	}

	writer, _, err := s.toType(info)
	if err != nil {
		return err
	}

	_, err = avro.Unmarshal(payload[6:], v, writer)
	return err
}

// DeserializeIntoRecordName implements deserialization of generic Avro data
func (s *GenericDeserializer) DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error {
	if payload == nil {
		return fmt.Errorf("Empty payload")
	}

	info, err := s.GetSchema("", payload)
	if err != nil {
		return err
	}

	// recreate the fullyQualifiedName
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(info.Schema), &data); err != nil {
		return err
	}
	name := data["name"].(string)
	namespace := data["namespace"].(string)
	msgFullyQlfName := fmt.Sprintf("%s.%s", namespace, name)

	msgFullyQlfNameValue, err := s.SubjectNameStrategy("", s.SerdeType, msgFullyQlfName)
	if err != nil {
		return err
	}

	var sub []string
	for _, s := range info.Subject {
		if s == msgFullyQlfNameValue {
			sub = append(sub, s)
			break
		}
	}
	if len(sub) == 0 {
		// retry with updating the cache
		_, err = s.retryGetSubjects(payload, sub, msgFullyQlfNameValue)
		if err != nil {
			return err
		}
		if len(subjects) == 0 {
			return fmt.Errorf("no subject found for: %v", msgFullyQlfNameValue)
		}
	}

	v, ok := subjects[sub[0]]
	if !ok {
		return fmt.Errorf("unfound subject declaration")
	}

	writer, _, err := s.toType(info)
	if err != nil {
		return err
	}

	_, err = avro.Unmarshal(payload[6:], v, writer)
	return err
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

	writer, name, err := s.toType(info)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType)
	if err != nil {
		return nil, err
	}
	var subjects = []string{subject}
	msg, err := s.MessageFactory(subjects, name)
	if err != nil {
		return nil, err
	}

	if msg == struct{}{} {
		codec, err := goavro.NewCodec(info.Schema)
		if err != nil {
			return nil, err
		}

		native, _, err := codec.NativeFromBinary(payload[6:])
		if err != nil {
			return nil, err
		}

		return native, nil
	}

	_, err = avro.Unmarshal(payload[6:], msg, writer)
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
	_, err = avro.Unmarshal(payload[6:], msg, writer)
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

func (s *GenericDeserializer) avroMessageFactory(subject []string, name string) (interface{}, error) {

	return struct{}{}, nil
}

// From https://stackoverflow.com/questions/42664837/how-to-access-unexported-struct-fields/43918797#43918797
func setPrivateAvroType(t *avro.Type, avroType schema.AvroType) {
	rt := reflect.ValueOf(t).Elem()
	rf := rt.Field(0)
	reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(avroType))
}
