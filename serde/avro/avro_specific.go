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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/actgardner/gogen-avro/v10/vm"
	"github.com/actgardner/gogen-avro/v10/vm/types"
	schemaregistry "github.com/djedjethai/gokfk-regent"
	"github.com/djedjethai/gokfk-regent/serde"
	"github.com/linkedin/goavro"
)

// SpecificSerializer represents a specific Avro serializer
type SpecificSerializer struct {
	serde.BaseSerializer
}

// SpecificDeserializer represents a specific Avro deserializer
type SpecificDeserializer struct {
	serde.BaseDeserializer
}

var _ serde.Serializer = new(SpecificSerializer)
var _ serde.Deserializer = new(SpecificDeserializer)

// SpecificAvroMessage represents a generated Avro class from gogen-avro
type SpecificAvroMessage interface {
	types.Field
	Serialize(w io.Writer) error
	Schema() string
}

// NewSpecificSerializer creates an Avro serializer for Avro-generated objects
func NewSpecificSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*SpecificSerializer, error) {
	s := &SpecificSerializer{}
	err := s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of specific Avro data
func (s *SpecificSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be an avro message. Got '%v'", t)
	}
	var id = 0
	info := schemaregistry.SchemaInfo{
		Schema: avroMsg.Schema(),
	}
	id, fromSR, err := s.GetID(topic, avroMsg, info)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = avroMsg.Serialize(&buf)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, fromSR, buf.Bytes())
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *SpecificSerializer) addFullyQualifiedNameToSchema(avroStr string, msg interface{}) ([]byte, string, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(avroStr), &data); err != nil {
		fmt.Println("Error unmarshaling JSON:", err)
	}

	var fullyQualifiedName string
	parts := strings.Split(data["name"].(string), ".")
	if len(parts) > 0 {
		var namespace string
		if len(parts) == 1 {
			// avro schema does not define a namespace, use the Go fullyQualifiedName
			msgFQNGo := reflect.TypeOf(msg).String()
			msgFQNGo = strings.TrimLeft(msgFQNGo, "*")
			partsMsg := strings.Split(msgFQNGo, ".")
			if len(partsMsg) > 2 {
				for i := 0; i < len(partsMsg)-1; i++ {
					if i == 0 {
						namespace += parts[0]
					} else {
						namespace += fmt.Sprintf(".%v", parts[i])
					}
				}
			} else {
				namespace = partsMsg[0]
			}
		} else if len(parts) == 2 {
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
		data["name"] = parts[len(parts)-1]
		data["namespace"] = namespace
		fullyQualifiedName = fmt.Sprintf("%v.%v", namespace, data["name"])
	}
	modifiedJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fullyQualifiedName, err
	}

	return modifiedJSON, fullyQualifiedName, nil
}

// SerializeRecordName implements serialization of generic Avro data
func (s *SpecificSerializer) SerializeRecordName(msg interface{}, subject ...string) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be an avro message. Got '%v'", t)
	}

	modifiedJSON, msgFQN, err := s.addFullyQualifiedNameToSchema(avroMsg.Schema(), msg)
	if err != nil {
		fmt.Println("Error marshaling JSON when adding fullyQualifiedName:", err)
	}

	if len(subject) > 0 {
		if msgFQN != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, msgFQN, subject[0])
		}
	}

	var id = 0
	info := schemaregistry.SchemaInfo{
		Schema: string(modifiedJSON),
	}

	var fromSR int
	id, fromSR, err = s.GetID(msgFQN, avroMsg, info)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = avroMsg.Serialize(&buf)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, fromSR, buf.Bytes())
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// SerializeTopicRecordName implements serialization of generic Avro data
func (s *SpecificSerializer) SerializeTopicRecordName(topic string, msg interface{}, subject ...string) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be an avro message. Got '%v'", t)
	}

	modifiedJSON, topicFQN, err := s.addFullyQualifiedNameToSchema(avroMsg.Schema(), msg)
	if err != nil {
		fmt.Println("Error marshaling JSON when adding fullyQualifiedName:", err)
	}

	topicFQN = fmt.Sprintf("%s-%s", topic, topicFQN)

	if len(subject) > 0 {
		if topicFQN != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, topicFQN, subject[0])
		}
	}

	var id = 0
	info := schemaregistry.SchemaInfo{
		Schema: string(modifiedJSON),
	}

	var fromSR int
	id, fromSR, err = s.GetID(topicFQN, avroMsg, info)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = avroMsg.Serialize(&buf)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, fromSR, buf.Bytes())
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewSpecificDeserializer creates an Avro deserializer for Avro-generated objects
func NewSpecificDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*SpecificDeserializer, error) {
	s := &SpecificDeserializer{}
	err := s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	s.MessageFactory = s.avroMessageFactory
	return s, nil
}

// DeserializeTopicRecordName implements deserialization of specific Avro data
func (s *SpecificDeserializer) DeserializeTopicRecordName(topic string, payload []byte) (interface{}, error) {
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
		fmt.Println("Error unmarshaling JSON:", err)
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

	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be an avro message. Got '%v'", t)
	}
	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return nil, err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return nil, err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(payload[6:])

	if err = vm.Eval(r, deser, avroMsg); err != nil {
		return nil, err
	}
	return avroMsg, nil

}

func (s *SpecificDeserializer) retryGetSubjects(payload []byte, subjects []string, topicMFQNValue string) ([]string, error) {
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

// DeserializeRecordName implements deserialization of specific Avro data
func (s *SpecificDeserializer) DeserializeRecordName(payload []byte) (interface{}, error) {
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
		fmt.Println("Error unmarshaling JSON:", err)
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

	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be an avro message. Got '%v'", t)
	}
	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return nil, err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return nil, err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(payload[6:])

	if err = vm.Eval(r, deser, avroMsg); err != nil {
		return nil, err
	}
	return avroMsg, nil
}

// DeserializeIntoTopicRecordName implements deserialization of specific Avro data
func (s *SpecificDeserializer) DeserializeIntoTopicRecordName(topic string, subjects map[string]interface{}, payload []byte) error {
	if payload == nil {
		return nil
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

	var avroMsg SpecificAvroMessage
	switch t := v.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return fmt.Errorf("deserialization target must be an avro message. Got '%v'", t)
	}
	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return err
	}

	r := bytes.NewReader(payload[6:])

	if err = vm.Eval(r, deser, avroMsg); err != nil {
		return err
	}

	return nil
}

// DeserializeIntoRecordName implements deserialization of specific Avro data
func (s *SpecificDeserializer) DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error {
	if payload == nil {
		return nil
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

	var avroMsg SpecificAvroMessage
	switch t := v.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return fmt.Errorf("deserialization target must be an avro message. Got '%v'", t)
	}

	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return err
	}
	r := bytes.NewReader(payload[6:])

	if err = vm.Eval(r, deser, avroMsg); err != nil {
		return err
	}

	return nil
}

// Deserialize implements deserialization of specific Avro data
func (s *SpecificDeserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType)
	if err != nil {
		return nil, err
	}

	var subjects = []string{subject}
	msg, err := s.MessageFactory(subjects, writer.Name())
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

	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be an avro message. Got '%v'", t)
	}
	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return nil, err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(payload[6:])

	if err = vm.Eval(r, deser, avroMsg); err != nil {
		return nil, err
	}
	return avroMsg, nil
}

// DeserializeInto implements deserialization of specific Avro data to the given object
func (s *SpecificDeserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return fmt.Errorf("serialization target must be an avro message. Got '%v'", t)
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return err
	}
	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return err
	}
	r := bytes.NewReader(payload[6:])
	return vm.Eval(r, deser, avroMsg)
}

func (s *SpecificDeserializer) toAvroType(schema schemaregistry.SchemaInfo) (schema.AvroType, error) {
	ns := parser.NewNamespace(false)
	return resolveAvroReferences(s.Client, schema, ns)
}

func (s *SpecificDeserializer) avroMessageFactory(subject []string, name string) (interface{}, error) {
	return struct{}{}, nil
}
