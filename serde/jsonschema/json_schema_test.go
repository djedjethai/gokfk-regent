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
	"testing"

	schemaregistry "github.com/djedjethai/kfk-schemaregistry"
	"github.com/djedjethai/kfk-schemaregistry/serde"
	"github.com/djedjethai/kfk-schemaregistry/test"
)

func TestJSONSchemaSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{0, 0, 0, 1}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestJSONSchemaSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	nested := JSONDemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{0, 0, 0, 1}
	obj := JSONNestedTestRecord{
		OtherField: nested,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

type JSONDemoSchema struct {
	IntField int32 `json:"IntField"`

	DoubleField float64 `json:"DoubleField"`

	StringField string `json:"StringField"`

	BoolField bool `json:"BoolField"`

	BytesField test.Bytes `json:"BytesField"`
}

type JSONNestedTestRecord struct {
	OtherField JSONDemoSchema
}

type JSONLinkedList struct {
	Value int32
	Next  *JSONLinkedList
}

// const (
// 	linkedList    = "test.LinkedList"
// 	linkedListRN  = "test.LinkedList:recordName"
// 	pizza         = "test.Pizza"
// 	pizzaRN       = "test.Pizza:recordName"
// 	invalidSchema = "invalidSchema"
// )
//
// var (
// 	inner = test.LinkedList{
// 		Value: 100,
// 	}
//
// 	obj = test.Pizza{
// 		Size:     "Extra extra large",
// 		Toppings: []string{"anchovies", "mushrooms"},
// 	}
//
// 	invalidObj = test.Author{
// 		Name: "Author name",
// 	}
// )
//
// func TestProtobufSerdeDeserializeRecordName(t *testing.T) {
// 	serde.MaybeFail = serde.InitFailFunc(t)
// 	var err error
// 	conf := schemaregistry.NewConfig("mock://")
//
// 	client, err := schemaregistry.NewClient(conf)
// 	serde.MaybeFail("Schema Registry configuration", err)
//
// 	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
// 	serde.MaybeFail("Serializer configuration", err)
//
// 	bytesInner, err := ser.SerializeRecordName(linkedListRN, &inner)
// 	serde.MaybeFail("serialization", err)
//
// 	bytesObj, err := ser.SerializeRecordName(pizzaRN, &obj)
// 	serde.MaybeFail("serialization", err)
//
// 	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
//
// 	serde.MaybeFail("Deserializer configuration", err)
// 	deser.Client = ser.Client
//
// 	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
// 	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
//
// 	newobj, err := deser.DeserializeRecordName(bytesInner)
// 	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), inner.ProtoReflect()))
//
// 	newobj, err = deser.DeserializeRecordName(bytesObj)
// 	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
// }
//
// func RegisterMessageFactory() func(string, string) (interface{}, error) {
// 	return func(subject string, name string) (interface{}, error) {
// 		switch name {
// 		case linkedList:
// 			return &test.LinkedList{}, nil
// 		case pizza:
// 			return &test.Pizza{}, nil
// 		}
// 		return nil, errors.New("No matching receiver")
// 	}
// }
//
// func RegisterMessageFactoryError() func(string, string) (interface{}, error) {
// 	return func(subject string, name string) (interface{}, error) {
// 		return nil, errors.New("No matching receiver")
// 	}
// }
//
// func RegisterMessageFactoryInvalidReceiver() func(string, string) (interface{}, error) {
// 	return func(subject string, name string) (interface{}, error) {
// 		switch name {
// 		case pizza:
// 			return &test.LinkedList{}, nil
// 		}
// 		return nil, errors.New("No matching receiver")
// 	}
// }
//
// func TestProtobufSerdeDeserializeRecordNameWithHandler(t *testing.T) {
// 	serde.MaybeFail = serde.InitFailFunc(t)
// 	var err error
// 	conf := schemaregistry.NewConfig("mock://")
//
// 	client, err := schemaregistry.NewClient(conf)
// 	serde.MaybeFail("Schema Registry configuration", err)
//
// 	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
// 	serde.MaybeFail("Serializer configuration", err)
//
// 	bytesInner, err := ser.SerializeRecordName(linkedListRN, &inner)
// 	serde.MaybeFail("serialization", err)
//
// 	bytesObj, err := ser.SerializeRecordName(pizzaRN, &obj)
// 	serde.MaybeFail("serialization", err)
//
// 	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
//
// 	serde.MaybeFail("Deserializer configuration", err)
// 	deser.Client = ser.Client
// 	deser.MessageFactory = RegisterMessageFactory()
//
// 	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
// 	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
//
// 	newobj, err := deser.DeserializeRecordName(bytesInner)
// 	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*test.LinkedList).Value, inner.Value))
//
// 	newobj, err = deser.DeserializeRecordName(bytesObj)
// 	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*test.Pizza).Size, obj.Size))
// 	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*test.Pizza).Toppings[0], obj.Toppings[0]))
// 	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*test.Pizza).Toppings[1], obj.Toppings[1]))
// }
//
// func TestProtobufSerdeDeserializeRecordNameWithHandlerInvalidReceiver(t *testing.T) {
// 	serde.MaybeFail = serde.InitFailFunc(t)
// 	var err error
// 	conf := schemaregistry.NewConfig("mock://")
//
// 	client, err := schemaregistry.NewClient(conf)
// 	serde.MaybeFail("Schema Registry configuration", err)
//
// 	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
// 	serde.MaybeFail("Serializer configuration", err)
//
// 	bytesObj, err := ser.SerializeRecordName(pizzaRN, &obj)
// 	serde.MaybeFail("serialization", err)
//
// 	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
//
// 	serde.MaybeFail("Deserializer configuration", err)
// 	deser.Client = ser.Client
// 	deser.MessageFactory = RegisterMessageFactoryError()
//
// 	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
// 	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
//
// 	newobj, err := deser.DeserializeRecordName(bytesObj)
//
// 	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "No matching receiver"))
// 	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
// }
//
// func TestProtobufSerdeDeserializeRecordNameWithInvalidSchema(t *testing.T) {
// 	serde.MaybeFail = serde.InitFailFunc(t)
// 	var err error
// 	conf := schemaregistry.NewConfig("mock://")
//
// 	client, err := schemaregistry.NewClient(conf)
// 	serde.MaybeFail("Schema Registry configuration", err)
//
// 	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
// 	serde.MaybeFail("Serializer configuration", err)
//
// 	bytesObj, err := ser.SerializeRecordName(pizzaRN, &obj)
// 	serde.MaybeFail("serialization", err)
//
// 	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
//
// 	serde.MaybeFail("Deserializer configuration", err)
// 	deser.Client = ser.Client
// 	deser.MessageFactory = RegisterMessageFactoryInvalidReceiver()
//
// 	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
// 	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
//
// 	_, err = deser.DeserializeRecordName(bytesObj)
//
// 	serde.MaybeFail("deserialization", err)
// }
//
// func TestProtobufSerdeDeserializeIntoRecordName(t *testing.T) {
// 	serde.MaybeFail = serde.InitFailFunc(t)
// 	var err error
// 	conf := schemaregistry.NewConfig("mock://")
//
// 	client, err := schemaregistry.NewClient(conf)
// 	serde.MaybeFail("Schema Registry configuration", err)
//
// 	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
// 	serde.MaybeFail("Serializer configuration", err)
//
// 	bytesInner, err := ser.SerializeRecordName(linkedListRN, &inner)
// 	serde.MaybeFail("serialization", err)
//
// 	bytesObj, err := ser.SerializeRecordName(pizzaRN, &obj)
// 	serde.MaybeFail("serialization", err)
//
// 	var receivers = make(map[string]interface{})
// 	receivers[linkedList] = &test.LinkedList{}
// 	receivers[pizza] = &test.Pizza{}
//
// 	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
//
// 	serde.MaybeFail("Deserializer configuration", err)
// 	deser.Client = ser.Client
//
// 	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
// 	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
//
// 	err = deser.DeserializeIntoRecordName(receivers, bytesInner)
// 	serde.MaybeFail("deserialization", err, serde.Expect(int(receivers[linkedList].(*test.LinkedList).Value), 100))
//
// 	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
// 	serde.MaybeFail("deserialization", err, serde.Expect(receivers[pizza].(*test.Pizza).Toppings[0], obj.Toppings[0]))
// 	serde.MaybeFail("deserialization", err, serde.Expect(receivers[pizza].(*test.Pizza).Toppings[1], obj.Toppings[1]))
// }
//
// func TestProtobufSerdeDeserializeIntoRecordNameWithInvalidSchema(t *testing.T) {
// 	serde.MaybeFail = serde.InitFailFunc(t)
// 	var err error
// 	conf := schemaregistry.NewConfig("mock://")
//
// 	client, err := schemaregistry.NewClient(conf)
// 	serde.MaybeFail("Schema Registry configuration", err)
//
// 	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
// 	serde.MaybeFail("Serializer configuration", err)
//
// 	bytesObj, err := ser.SerializeRecordName(pizzaRN, &obj)
// 	serde.MaybeFail("serialization", err)
//
// 	var receivers = make(map[string]interface{})
// 	receivers[invalidSchema] = &test.Pizza{}
//
// 	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
//
// 	serde.MaybeFail("Deserializer configuration", err)
// 	deser.Client = ser.Client
//
// 	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
// 	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
//
// 	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
// 	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration for test.Pizza"))
// 	serde.MaybeFail("deserialization", serde.Expect(receivers[invalidSchema].(*test.Pizza).Size, ""))
// }
//
// func TestProtobufSerdeDeserializeIntoRecordNameWithInvalidReceiver(t *testing.T) {
// 	serde.MaybeFail = serde.InitFailFunc(t)
// 	var err error
// 	conf := schemaregistry.NewConfig("mock://")
//
// 	client, err := schemaregistry.NewClient(conf)
// 	serde.MaybeFail("Schema Registry configuration", err)
//
// 	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
// 	serde.MaybeFail("Serializer configuration", err)
//
// 	bytesObj, err := ser.SerializeRecordName(pizzaRN, &obj)
// 	serde.MaybeFail("serialization", err)
//
// 	var receivers = make(map[string]interface{})
// 	receivers[pizza] = &test.LinkedList{}
//
// 	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
//
// 	serde.MaybeFail("Deserializer configuration", err)
// 	deser.Client = ser.Client
//
// 	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
// 	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
//
// 	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
// 	serde.MaybeFail("deserialization", serde.Expect(err.Error(), `recipient proto object 'LinkedList' differs from incoming events`))
// }
//
