# gokfk-regent: A Comprehensive Go Kafka Schema Registry Client

**gokfk-regent** gok(a)fk(a)-reg(istry-cli)ent, is a Go Kafka Schema Registry Client that encompasses all strategies, including Topic Name, Record Name, and Topic Record Name.

It's a fork of Confluent-kafka-go's SchemaRegistry, inheriting the Topic Name Strategy and introducing the Record Name Strategy and Topic Record Name Strategy, currently absent in the original client. This implementation enables Go developers to leverage these strategies.

As an open-source project, we prioritize flexibility, allowing for rapid development, and warmly welcome contributions from the community.

Find a detailed presentation of the project [here](https://medium.com/@djedjethai/go-client-for-the-kafka-schema-registry-gokfk-regent-c8b9f534b090).


## Explore the Examples Section
* Full implementations using gokfk-regent, confluentinc images, and confluent-kafka-go/kafka client
* Full implementations using gokfk-regent, wurstmeister images, and segmentio/kafka client


## Implemented
* TopicNameStrategy for ProtoBuf, JsonSchema, Avro(from the parent project, Confluent-kafka-go)
* RecordNameStrategy for Protobuf, JsonSchema, Avro  
* TopicRecordNameStrategy for Protobuf, JsonSchema, Avro 

## Install
``` bash
$ go get github.com/djedjethai/gokfk-regent
```

## Testing
```
go test -v -cover ./... 
```

## Features
* Topic-Name-Strategy interface(breaking change with the mother repo if register MessageFactory())
```
Serialize(topic string, msg interface{}) ([]byte, error)
Deserialize(topic string, payload []byte) (interface{}, error)
DeserializeInto(topic string, payload []byte, msg interface{}) error
```

* Record-Name-Strategy interface
```
SerializeRecordName(msg interface{}, subject ...string) ([]byte, error)
DeserializeRecordName(payload []byte) (interface{}, error)
DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error
```

* Topic-Record-Name-Strategy interface
```
SerializeTopicRecordName(topic string, msg interface{}, subject ...string) ([]byte, error)
DeserializeTopicRecordName(topic string, payload []byte) (interface{}, error)
DeserializeIntoTopicRecordName(topic string, subjects map[string]interface{}, payload []byte) error
```

* The default MessageFactory() handler can be overridden.
```
// using the name
deserializer.MessageFactory = RegisterMessageFactory()
func RegisterMessageFactory() func([]string, string) (interface{}, error) {
	return func(subjects []string, name string) (interface{}, error) {
		switch name {
		case "packageName.Person":
			return &pb.Person{}, nil
		case "packageName.Address":
			return &pb.Address{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}

// using the subject
deserializer.MessageFactory = RegisterMessageFactoryOnSubject()
func RegisterMessageFactoryOnSubject() func([]string, string) (interface{}, error) {
	return func(subjects []string, name string) (interface{}, error) {
		switch subjects[0] {
		case "topic-packageName.Person-key":
			return &pb.PersonKey{}, nil
		case "topic-packageName.Person-value":
			return &pb.Person{}, nil
		case "anotherTopic-packageName.Address-value":
			return &pb.Address{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}
```

## Use the TopicRecordNameStrategy(DeserializeTopicRecordName()) with Protobuf, using segmentio, wurstmeister, gokfk-regent
(see in ./examples for the full implementation)
```
/*
* ===============================
* PRODUCER
* ===============================
**/
w := &kafka.Writer{
	Addr:     kafka.TCP(kafkaURL),
	Topic:    topic,
	Balancer: &kafka.LeastBytes{},
}

// protobuf package "personPackage"
msg := &pb.Person{
	Name: "robert",
	Age:  23,
}

c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
if err != nil {
	return nil, err
}

s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
if err != nil {
	return nil, err
}

// Write a Person to the topic "topic-personPackage.Person."
// The use of "topic-personPackage.Person" is optional but useful, as it asserts the subject's message.
// It can also be used for "topic-personPackage.Person-key" or "topic-personPackage.Person-value."
payload, err := s.SerializeTopicRecordName(topic, msg, "topic-personPackage.Person")
if err != nil {
	return err
}
kfkmsg := kafka.Message{
    Key:   []byte(key),
    Value: payload,
}
err = w.WriteMessages(context.Background(), kfkmsg)
if err != nil {
	fmt.Println(err)
	return err
}


/*
* ===============================
* CONSUMER
* ===============================
**/
rTopic := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
})


sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
if err != nil {
	return nil, err
}

d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
if err != nil {
	return nil, err
}

// This is not needed if the default MessageFactory() function is overwritten. See examples for clarification.
 jobType := (&pb.Job{}).ProtoReflect().Type()

d.ProtoRegistry.RegisterMessage(jobType)

topicMsg, err := rTopic.ReadMessage(context.Background())
if err != nil {
	log.Fatalln(err)
}

msg, err := d.DeserializeTopicRecordName(topic, topicMsg.Value)
if err != nil {
	return err
}

fmt.Printf("message %v\n", msg)
``` 

## Use the RecordNameStrategy(DeserializeIntoRecordName()) with AvroSchema, using confluent-kafka-go, confluentinc, gokfk-regent
(see in ./examples for the full implementation)
```
/*
* ===============================
* PRODUCER
* ===============================
**/
producer, err := kafka.NewProducer(
	&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:29092",
})

// avro namespace is "avroNamespace"
msg := &avSch.Person{
	Name: "robert",
	Age:  23,
}

c, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
if err != nil {
	log.Fatal("Error schemaRegistry create new client: ", err)
}

s, err := avro.NewSpecificSerializer(c, serde.ValueSerde, avro.NewSerializerConfig())
if err != nil {
	log.Fatal("Error creating the serializer: ", err)
}

// The use of "avroNamespace.Person" is optional but useful, as it asserts the subject's message.
payloadMsg, err := s.SerializeRecordName(&msg, "avroNamespace.Person")
if err != nil {
	log.Println("Error serializing the msg")
}

msgOK := &kafka.Message{
	TopicPartition: kafka.TopicPartition{
		Topic:     &topic,
		Partition: kafka.PartitionAny},
	Value: payloadMsg}

// Produce the message
err = producer.Produce(msgOK, nil)
if err != nil {
    fmt.Printf("Failed to produce message: %v\n", err)
}

/*
* ===============================
* CONSUMER
* ===============================
**/
c, err := kafka.NewConsumer(&kafka.ConfigMap{
	"bootstrap.servers":  kafkaURL,
	"group.id":           consumerGroupID,
	"session.timeout.ms": defaultSessionTimeout,
	"enable.auto.commit": false,
})
if err != nil {
	return nil, err
}

if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
    return err
}

sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
if err != nil {
	return nil, err
}

d, err := avro.NewSpecificDeserializer(sr, serde.ValueSerde, avro.NewDeserializerConfig())
if err != nil {
	return nil, err
}

ref := make(map[string]interface{})
px := avSch.Person{}
msgFQN := "avroNamespace.Person"
ref[msgFQN] = &px

for {
	kafkaMsg, err := c.ReadMessage(noTimeout)
	if err != nil {
		return err
	}

	// use deserializer.DeserializeIntoRecordName to deserialize into the receiver "ref"
	err = d.DeserializeIntoRecordName(ref, kafkaMsg.Value)
	if err != nil {
		return err
	}
	fmt.Println("See the Person struct: ", px.Name, " - ", px.Age)

	if _, err = c.CommitMessage(kafkaMsg); err != nil {
		return err
	}
}
```


## Contributing

"Welcome to gokfk-regent! If you feel that it can be improved, please feel free to submit a pull request (PR)."


License
=======

[Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)
