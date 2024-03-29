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

package jsonschema

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"

	schemaregistry "github.com/djedjethai/gokfk-regent"
	"github.com/djedjethai/gokfk-regent/serde"
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

	jschema := jsonschema.Reflect(msg)

	raw, err := json.Marshal(jschema)
	if err != nil {
		return nil, err
	}

	info := schemaregistry.SchemaInfo{
		Schema:     string(raw),
		SchemaType: "JSON",
	}

	id, fromSR, err := s.GetID(topic, msg, info)
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
	payload, err := s.WriteBytes(id, fromSR, raw)
	if err != nil {
		return nil, err
	}
	return payload, nil

}

func (s *Serializer) addFullyQualifiedNameToSchema(jsonBytes []byte, msgFQN string) ([]byte, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return nil, err
	}

	parts := strings.Split(msgFQN, ".")
	if len(parts) > 0 {
		var namespace string
		var name string
		if len(parts) == 2 {
			namespace = parts[0]
			name = parts[1]
		} else if len(parts) > 2 {
			for i := 0; i < len(parts)-1; i++ {
				if i == 0 {
					namespace += parts[0]
				} else {
					namespace += fmt.Sprintf(".%v", parts[i])
				}
			}
			name = parts[len(parts)-1]

		}
		data["name"] = name
		data["namespace"] = namespace
	}

	return json.Marshal(data)
}

// SerializeRecordName implements serialization of generic data to JSON
func (s *Serializer) SerializeRecordName(msg interface{}, subject ...string) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	// get the fully qualified name
	msgFQN := reflect.TypeOf(msg).String()
	msgFQN = strings.TrimLeft(msgFQN, "*") // in case

	if len(subject) > 0 {
		if msgFQN != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, msgFQN, subject[0])
		}
	}

	jschema := jsonschema.Reflect(msg)

	// Marshal the schema into a JSON []byte
	schemaBytes, err := json.Marshal(jschema)
	if err != nil {
		return nil, err
	}

	raw, err := s.addFullyQualifiedNameToSchema(schemaBytes, msgFQN)
	if err != nil {
		log.Println("Error marshaling JSON when adding fullyQualifiedName:", err)
	}

	info := schemaregistry.SchemaInfo{
		Schema:     string(raw),
		SchemaType: "JSON",
	}

	id, fromSR, err := s.GetID(msgFQN, msg, info)
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
	payload, err := s.WriteBytes(id, fromSR, raw)
	if err != nil {
		return nil, err
	}
	return payload, nil

}

// SerializeTopicRecordName implements serialization of generic data to JSON
func (s *Serializer) SerializeTopicRecordName(topic string, msg interface{}, subject ...string) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	// get the fully qualified name
	msgFQN := reflect.TypeOf(msg).String()
	msgFQN = strings.TrimLeft(msgFQN, "*") // in case

	// Marshal the schema into a JSON []byte
	jschema := jsonschema.Reflect(msg)
	schemaBytes, err := json.Marshal(jschema)
	if err != nil {
		return nil, err
	}
	raw, err := s.addFullyQualifiedNameToSchema(schemaBytes, msgFQN)
	if err != nil {
		log.Println("Error marshaling JSON when adding fullyQualifiedName:", err)
	}

	// add topic to the fullyQualifiedName
	msgFQN = fmt.Sprintf("%s-%s", topic, msgFQN)

	if len(subject) > 0 {
		if msgFQN != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, msgFQN, subject[0])
		}
	}

	info := schemaregistry.SchemaInfo{
		Schema:     string(raw),
		SchemaType: "JSON",
	}

	id, fromSR, err := s.GetID(msgFQN, msg, info)
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
	payload, err := s.WriteBytes(id, fromSR, raw)
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
	s.MessageFactory = s.jsonMessageFactory
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
		err = json.Unmarshal(payload[6:], &obj)
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
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType)
	if err != nil {
		return nil, err
	}

	var subjects = []string{subject}
	msg, err := s.MessageFactory(subjects, "")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(payload[6:], msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// DeserializeTopicRecordName deserialise bytes
func (s *Deserializer) DeserializeTopicRecordName(topic string, payload []byte) (interface{}, error) {
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
		log.Println("Error unmarshaling JSON:", err)
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

	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[6:], &obj)
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

	msg, err := s.MessageFactory(subjects, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(payload[6:], msg)
	if err != nil {
		return nil, err
	}
	return msg, nil

}

func (s *Deserializer) retryGetSubjects(payload []byte, subjects []string, topicMFQNValue string) ([]string, error) {
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

// DeserializeRecordName deserialise bytes
func (s *Deserializer) DeserializeRecordName(payload []byte) (interface{}, error) {
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
		log.Println("Error unmarshaling JSON:", err)
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

	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[6:], &obj)
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

	msg, err := s.MessageFactory(subjects, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(payload[6:], msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// DeserializeIntoTopicRecordName deserialize bytes into the map interface{}
func (s *Deserializer) DeserializeIntoTopicRecordName(topic string, subjects map[string]interface{}, payload []byte) error {
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
		log.Println("Error unmarshaling JSON:", err)
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

	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[6:], &obj)
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

	err = json.Unmarshal(payload[6:], v)
	if err != nil {
		return err
	}
	return nil
}

// DeserializeIntoRecordName deserialize bytes into the map interface{}
func (s *Deserializer) DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error {
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
		log.Println("Error unmarshaling JSON:", err)
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

	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[6:], &obj)
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

	err = json.Unmarshal(payload[6:], v)
	if err != nil {
		return err
	}
	return nil
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
		err = json.Unmarshal(payload[6:], &obj)
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
	err = json.Unmarshal(payload[6:], msg)
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

func (s *Deserializer) jsonMessageFactory(subject []string, name string) (interface{}, error) {
	var msg map[string]interface{}
	return &msg, nil
}
