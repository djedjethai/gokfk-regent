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
	"errors"
	"fmt"
	"testing"

	schemaregistry "github.com/djedjethai/gokfk-regent"
	"github.com/djedjethai/gokfk-regent/serde"
	"github.com/djedjethai/gokfk-regent/test"
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

const (
	linkedList      = "jsonschema.LinkedList"
	linkedListValue = "jsonschema.LinkedList-value"
	pizza           = "jsonschema.Pizza"
	pizzaValue      = "jsonschema.Pizza-value"
	invalidSchema   = "invalidSchema"
)

type LinkedList struct {
	Value int
}

type Pizza struct {
	Size     string
	Toppings []string
}

type Author struct {
	Name string
}

var (
	inner = LinkedList{
		Value: 100,
	}

	obj = Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}
)

func TestJSONSerdeDeserializeRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj, pizza)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&map[Value:100]`))
	// access the newobj payload
	if objPtr, ok := newobj.(*map[string]interface{}); ok {
		// objPtr is now a pointer to a map[string]interface{}
		if objPtr != nil {
			// Dereference the pointer to access the map
			obj := *objPtr
			if value, ok := obj["Value"].(interface{}); ok {
				serde.MaybeFail("deserialization", serde.Expect(value.(float64), float64(100)))
			} else {
				fmt.Println("Value is not of type int")
			}
		} else {
			fmt.Println("objPtr is nil")
		}
	}

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&map[Size:Extra extra large Toppings:[anchovies mushrooms]]`))
}

func RegisterMessageFactory() func([]string, string) (interface{}, error) {
	return func(subject []string, name string) (interface{}, error) {
		switch name {
		case linkedList:
			return &LinkedList{}, nil
		case pizza:
			return &Pizza{}, nil
		}
		return nil, fmt.Errorf("No matching receiver")
	}
}

func RegisterMessageFactoryOnSubject() func([]string, string) (interface{}, error) {
	return func(subject []string, name string) (interface{}, error) {
		for _, s := range subject {
			switch s {
			case pizzaValue:
				return &Pizza{}, nil
			case linkedListValue:
				return &LinkedList{}, nil
			}
		}
		return nil, fmt.Errorf("No matching receiver")
	}
}

func RegisterMessageFactoryNoReceiver() func([]string, string) (interface{}, error) {
	return func(subject []string, name string) (interface{}, error) {
		return nil, fmt.Errorf("No matching receiver")
	}
}

func RegisterMessageFactoryInvalidReceiver() func([]string, string) (interface{}, error) {
	return func(subject []string, name string) (interface{}, error) {
		switch name {
		case pizza:
			return &LinkedList{}, nil
		case linkedList:
			return "", nil
		}
		return nil, fmt.Errorf("No matching receiver")
	}
}

func TestJSONSerdeDeserializeRecordNameWithHandler(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner, linkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = RegisterMessageFactory()

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*LinkedList).Value, inner.Value))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Size, obj.Size))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[1], obj.Toppings[1]))
}

func TestJSONSerdeDeserializeRecordNameWithHandlerOnSubject(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner, linkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = RegisterMessageFactoryOnSubject()

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*LinkedList).Value, inner.Value))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Size, obj.Size))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[1], obj.Toppings[1]))
}

func TestJSONSerdeDeserializeRecordNameWithHandlerNoReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid receiver
	deser.MessageFactory = RegisterMessageFactoryNoReceiver()

	newobj, err := deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "No matching receiver"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestJSONSerdeDeserializeRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid schema
	deser.MessageFactory = RegisterMessageFactoryInvalidReceiver()

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "json: Unmarshal(non-pointer string)"))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", err)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&{0}`))
}

func TestJSONSerdeDeserializeIntoRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj, pizza)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[linkedListValue] = &LinkedList{}
	receivers[pizzaValue] = &Pizza{}

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(int(receivers[linkedListValue].(*LinkedList).Value), 100))

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[pizzaValue].(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[pizzaValue].(*Pizza).Toppings[1], obj.Toppings[1]))
}

func TestJSONSerdeDeserializeIntoRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[invalidSchema] = &Pizza{}

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration"))
	serde.MaybeFail("deserialization", serde.Expect(receivers[invalidSchema].(*Pizza).Size, ""))
}

func TestJSONSerdeDeserializeIntoRecordNameWithInvalidReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	bytesInner, err := ser.SerializeRecordName(&inner, linkedList)
	serde.MaybeFail("serialization", err)

	aut := Author{
		Name: "aut",
	}
	bytesAut, err := ser.SerializeRecordName(&aut)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[pizzaValue] = &LinkedList{}
	receivers[linkedListValue] = ""

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", receivers[pizzaValue]), `&{0}`))

	err = deser.DeserializeIntoRecordName(receivers, bytesInner)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "json: Unmarshal(non-pointer string)"))
	err = deser.DeserializeIntoRecordName(receivers, bytesAut)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration"))
}

func TestJSONSerdeRecordNamePayloadUnmatchSubject(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	_, err = ser.SerializeRecordName(&obj, "test.Pizza")
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "the payload's fullyQualifiedName: 'jsonschema.Pizza' does not match the subject: 'test.Pizza'"))
}

// --------------------- topicRecordName -----------------

const (
	topic                 = "topic"
	second                = "second"
	topicLinkedList       = "topic-jsonschema.LinkedList"
	topicLinkedListValue  = "topic-jsonschema.LinkedList-value"
	secondLinkedList      = "second-jsonschema.LinkedList"
	secondLinkedListValue = "second-jsonschema.LinkedList-value"
	topicPizza            = "topic-jsonschema.Pizza"
	topicPizzaValue       = "topic-jsonschema.Pizza-value"
	secondPizza           = "second-jsonschema.Pizza"
	secondPizzaValue      = "second-jsonschema.Pizza-value"
)

func RegisterTRNMessageFactory() func([]string, string) (interface{}, error) {
	return func(subjects []string, name string) (interface{}, error) {
		// in json and avro we can switch on the name as the fullyQName is register
		switch name {
		// case topicLinkedList, secondLinkedList:
		case linkedList:
			return &LinkedList{}, nil
		case pizza:
			return &Pizza{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}

func RegisterTRNMessageFactoryOnSubject() func([]string, string) (interface{}, error) {
	return func(subjects []string, name string) (interface{}, error) {
		// in json and avro we can switch on the name as the fullyQName is register
		for _, s := range subjects {
			switch s {
			// case topicLinkedList, secondLinkedList:
			case topicLinkedListValue, secondLinkedListValue:
				return &LinkedList{}, nil
			case topicPizzaValue, secondPizzaValue:
				return &Pizza{}, nil
			}
		}
		return nil, errors.New("No matching receiver")
	}
}

func RegisterTRNMessageFactoryNoReceiver() func([]string, string) (interface{}, error) {
	return func(subject []string, name string) (interface{}, error) {
		return nil, errors.New("No matching receiver")
	}
}

func RegisterTRNMessageFactoryInvalidReceiver() func([]string, string) (interface{}, error) {
	return func(subjects []string, name string) (interface{}, error) {
		switch name {
		case linkedList:
			return "", nil
		case pizza:
			return &LinkedList{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}

func TestJSONSerdeDeserializeTopicRecordNameWithoutHandler(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeTopicRecordName(topic, &inner, topicLinkedList)
	serde.MaybeFail("serialization", err)

	// event inner is not a * it works
	bytesInner2, err := ser.SerializeTopicRecordName(second, inner, secondLinkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, &obj, topicPizza)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	newobj, err := deser.DeserializeTopicRecordName(topic, bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&map[Value:100]`))

	newobj, err = deser.DeserializeTopicRecordName(second, bytesInner2)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&map[Value:100]`))

	newobj, err = deser.DeserializeTopicRecordName(topic, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&map[Size:Extra extra large Toppings:[anchovies mushrooms]]`))

	newobj, err = deser.DeserializeTopicRecordName("invalid", bytesInner2)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "no subject found for: invalid-jsonschema.LinkedList-value"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestJSONSerdeDeserializeTopicRecordNameWithHandler(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeTopicRecordName(topic, &inner, topicLinkedList)
	serde.MaybeFail("serialization", err)

	// not that it does not matter &inner or inner
	bytesInner2, err := ser.SerializeTopicRecordName(second, inner, secondLinkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, obj, topicPizza)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = RegisterTRNMessageFactory()

	newobj, err := deser.DeserializeTopicRecordName(topic, bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*LinkedList).Value, inner.Value))

	newobj, err = deser.DeserializeTopicRecordName(second, bytesInner2)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*LinkedList).Value, inner.Value))

	newobj, err = deser.DeserializeTopicRecordName(topic, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Size, obj.Size))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[1], obj.Toppings[1]))

	newobj, err = deser.DeserializeTopicRecordName("invalid", bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "no subject found for: invalid-jsonschema.Pizza-value"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestJSONSerdeDeserializeTopicRecordNameWithHandlerOnSubject(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeTopicRecordName(topic, &inner, topicLinkedList)
	serde.MaybeFail("serialization", err)

	// not that it does not matter &inner or inner
	bytesInner2, err := ser.SerializeTopicRecordName(second, inner, secondLinkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, obj, topicPizza)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = RegisterTRNMessageFactoryOnSubject()

	newobj, err := deser.DeserializeTopicRecordName(topic, bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*LinkedList).Value, inner.Value))

	newobj, err = deser.DeserializeTopicRecordName(second, bytesInner2)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*LinkedList).Value, inner.Value))

	newobj, err = deser.DeserializeTopicRecordName(topic, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Size, obj.Size))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[1], obj.Toppings[1]))

	newobj, err = deser.DeserializeTopicRecordName("invalid", bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "no subject found for: invalid-jsonschema.Pizza-value"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestJSONSerdeDeserializeTopicRecordNameWithHandlerNoReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, &obj, topicPizza)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid receiver
	deser.MessageFactory = RegisterTRNMessageFactoryNoReceiver()

	newobj, err := deser.DeserializeTopicRecordName("invalid", bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "no subject found for: invalid-jsonschema.Pizza-value"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestJSONSerdeDeserializeTopicRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeTopicRecordName(topic, &inner, topicLinkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, &obj, topicPizza)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid schema
	deser.MessageFactory = RegisterTRNMessageFactoryInvalidReceiver()

	newobj, err := deser.DeserializeTopicRecordName(topic, bytesInner)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "json: Unmarshal(non-pointer string)"))

	newobj, err = deser.DeserializeTopicRecordName(topic, bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", err)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&{0}`))
}

func TestJSONSerdeDeserializeIntoTopicRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeTopicRecordName(topic, &inner, topicLinkedList)
	serde.MaybeFail("serialization", err)

	// note that it does not matter &inner or inner
	bytesInner2, err := ser.SerializeTopicRecordName(second, inner, secondLinkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, obj, topicPizza)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[topicLinkedListValue] = &LinkedList{}
	receivers[secondLinkedListValue] = &LinkedList{}
	receivers[topicPizzaValue] = &Pizza{}

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoTopicRecordName(topic, receivers, bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(int(receivers[topicLinkedListValue].(*LinkedList).Value), 100))

	err = deser.DeserializeIntoTopicRecordName(second, receivers, bytesInner2)
	serde.MaybeFail("deserialization", err, serde.Expect(int(receivers[secondLinkedListValue].(*LinkedList).Value), 100))

	err = deser.DeserializeIntoTopicRecordName(topic, receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[topicPizzaValue].(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[topicPizzaValue].(*Pizza).Toppings[1], obj.Toppings[1]))

	newobj, err := deser.DeserializeTopicRecordName("invalid", bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "no subject found for: invalid-jsonschema.Pizza-value"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestJSONSerdeDeserializeIntoTopicRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, obj, topicPizza)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[invalidSchema] = &Pizza{}

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoTopicRecordName(topic, receivers, bytesObj)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration"))
	serde.MaybeFail("deserialization", serde.Expect(receivers[invalidSchema].(*Pizza).Size, ""))
}

func TestJSONSerdeDeserializeIntoTopicRecordNameWithInvalidReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeTopicRecordName(topic, obj, topicPizza)
	serde.MaybeFail("serialization", err)

	bytesInner, err := ser.SerializeTopicRecordName(topic, &inner, topicLinkedList)
	serde.MaybeFail("serialization", err)

	aut := Author{
		Name: "aut",
	}
	bytesAut, err := ser.SerializeTopicRecordName(topic, &aut, "topic-jsonschema.Author")
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[topicPizzaValue] = &LinkedList{}
	receivers[topicLinkedListValue] = ""

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoTopicRecordName(topic, receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", receivers[topicPizzaValue]), `&{0}`))

	err = deser.DeserializeIntoTopicRecordName(topic, receivers, bytesInner)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "json: Unmarshal(non-pointer string)"))

	err = deser.DeserializeIntoTopicRecordName(topic, receivers, bytesAut)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration"))
}

func TestJSONSerdeTopicRecordNamePayloadUnmatchSubject(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	// should be "topic-jsonschema.Pizza"
	_, err = ser.SerializeTopicRecordName(topic, obj, "jsonschema.Pizza")
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "the payload's fullyQualifiedName: 'topic-jsonschema.Pizza' does not match the subject: 'jsonschema.Pizza'"))
}
