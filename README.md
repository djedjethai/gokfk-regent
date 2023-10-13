# gokfk-regent: A Comprehensive Go Kafka Schema Registry Client

**gokfk-regent** gok(a)fk(a)-reg(istry-cli)ent, is a Go Kafka Schema Registry Client that encompasses all strategies, including TopicName, RecordName, and TopicRecordName.

It is a fork of Confluent-kafka-go's SchemaRegistry, inheriting the Topic-Name-Strategy implementation while introducing the Record-Name-Strategy and Topic-Record-Name-Strategy, which are currently missing in the original client. This fork is tailored to users with specific functionality requirements, elevating their experience with Apache Kafka.

As an agile open-source project, we prioritize flexibility, allowing for rapid development, and warmly welcome contributions from the community.

## Explore the Examples Section
* Full implementations using gokfk-regent, confluentinc images, and confluent-kafka-go/kafka client
* Full implementations using gokfk-regent, wurstmeister images, and segmentio/kafka client

## Implemented
* TopicNameStrategy for ProtoBuf, JsonSchema, Avro(from the parent project, Confluent-kafka-go)
* RecordNameStrategy for Protobuf, JsonSchema, Avro  

## Working on
* Implementing the TopicRecordNameStrategy for Protobuf, Json and Avro

## Install
``` bash
$ go get github.com/djedjethai/gokfk-regent
```

## Features
* Topic-Name-Strategy interface(no breaking change with the mother repo, confluent-kafka-go/schemaregistry)
```
Serialize(topic string, msg interface{}) ([]byte, error)

// Deserialize will call the default MessageFactory to create an object
Deserialize(topic string, payload []byte) (interface{}, error)
// DeserializeInto will unmarshal data into the given msg object.
DeserializeInto(topic string, payload []byte, msg interface{}) error
```

* Record-Name-Strategy interface
```
// The optional subject assert the msg fullyQualifiedClassName with the expected subject
SerializeRecordName(msg interface{}, subject ...string) ([]byte, error)

// DeserializeRecordName will call the default MessageFactory to create an object
DeserializeRecordName(payload []byte) (interface{}, error)
// DeserializeIntoRecordName will unmarshal data into the given subjects' value(object).
DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error
```

* In the case of Deserialize() and DeserializeRecordName(), the default MessageFactory() handler can be overridden.
```
deserializer.MessageFactory = RegisterMessageFactory()

func RegisterMessageFactory() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch name {
		case subjectPerson:
			return &pb.Person{}, nil
		case subjectAddress:
			return &pb.Address{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}
```

## Use the RecordNameStrategy(DeserializeRecordName()) with Protobuf, using segmentio, wurstmeister, gokfk-regent
(see in ./examples for the full implementation)
```
/*
* ===============================
* PRODUCER
* ===============================
**/

func producer() {
	producer, err := NewProducer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	msg := &pb.Person{
		Name: "robert",
		Age:  23,
	}

	city := &pb.Address{
		Street: "myStreet",
		City:   "Bangkok",
	}

	for {
		err := producer.ProduceMessage(msg, topic, subjectPerson)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		err = producer.ProduceMessage(city, topic, subjectAddress)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		time.Sleep(2 * time.Second)
	}
}

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg proto.Message, topic, subject string) error {
    // That is enough !!!
	// payload, err := p.serializer.SerializeRecordName(msg)
	 
    // Or that will assert the msg fullyQualifiedClassName with the expected one
    payload, err := p.serializer.SerializeRecordName(msg, subject)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("Key-%d", count)

	kfkmsg := kafka.Message{
		Key:   []byte(key),
		Value: payload,
	}
	err = p.writer.WriteMessages(context.Background(), kfkmsg)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

/*
* ===============================
* CONSUMER
* ===============================
**/
// RegisterMessageFactory will overwrite the default one
// In this case &pb.Person{} is the "msg" at "msg, err := c.deserializer.DeserializeRecordName()"
func (c *srConsumer) RegisterMessageFactory() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch name {
		case subjectPerson:
			return &pb.Person{}, nil
		case subjectAddress:
			return &pb.Address{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}

// consumer
func (c *srConsumer) Run(messagesType []protoreflect.MessageType, topic string) error {

	if len(messagesType) > 0 {
		for _, mt := range messagesType {
			if err := c.deserializer.ProtoRegistry.RegisterMessage(mt); err != nil {

				return err
			}
		}
	}

    // register the MessageFactory() (if not, the default one is used)
    c.deserializer.MessageFactory = c.RegisterMessageFactory()

	for {
		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}

		msg, err := c.deserializer.DeserializeRecordName(m.Value)
		if err != nil {
			return err
		}

		// without RegisterMessageFactory()
		// c.handleMessageAsInterface(msg, int64(m.Offset))

		// with RegisterMessageFactory()
		if _, ok := msg.(*pb.Person); ok {
			fmt.Println("Person: ", msg.(*pb.Person).Name, " - ", msg.(*pb.Person).Age)
		} else {

			fmt.Println("Address: ", msg.(*pb.Address).City, " - ", msg.(*pb.Address).Street)
		}

		fmt.Printf("message at topic:%v partition:%v offset:%v	%s\n", m.Topic, m.Partition, m.Offset, string(m.Key))
	}

}

func (c *srConsumer) handleMessageAsInterface(message interface{}, offset int64) {
	fmt.Printf("message %v with offset %d\n", message, offset)
}
``` 

## Use the RecordNameStrategy(DeserializeIntoRecordName()) with AvroSchema, using confluent-kafka-go, confluentinc, gokfk-regent
(see in ./examples for the full implementation)
```
/*
* ===============================
* PRODUCER
* ===============================
**/

func producer() {
	producer, err := NewProducer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	msg := &avSch.Person{
		Name: "robert",
		Age:  23,
	}

	addr := &avSch.Address{
		Street: "rue de la soif",
		City:   "Rennes",
	}

	for {
		offset, err := producer.ProduceMessage(msg, topic, "personrecord.Person")
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		offset, err = producer.ProduceMessage(addr, topic, "addressrecord.Address")
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}

func (p *srProducer) ProduceMessage(msg interface{}, topic, subject string) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)

	// That is enough !!!
    // err := p.serializer.SerializeRecordName(msg)

    // Or that will assert the msg fullyQualifiedClassName with the expected one
	payload, err := p.serializer.SerializeRecordName(msg, subject)
	if err != nil {
		return nullOffset, err
	}
	if err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          payload,
	}, kafkaChan); err != nil {
		return nullOffset, err
	}
	e := <-kafkaChan
	switch ev := e.(type) {
	case *kafka.Message:
		log.Println("message sent: ", string(ev.Value))
		return int64(ev.TopicPartition.Offset), nil
	case kafka.Error:
		return nullOffset, err
	}
	return nullOffset, nil
}

/*
* ===============================
* CONSUMER
* ===============================
**/
// Run consumer
func (c *srConsumer) Run(topic string) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}

	// case DeserializeIntoRecordName(MessageFactory() handler is not called)
	ref := make(map[string]interface{})
	px := avSch.Person{}
	addr := avSch.Address{}
	msgFQN := "personrecord.Person"
	addrFQN := "addressrecord.Address"
	ref[msgFQN] = &px
	ref[addrFQN] = &addr

	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}

		// use deserializer.DeserializeInto to get a struct back
		err = c.deserializer.DeserializeIntoRecordName(ref, kafkaMsg.Value)
		if err != nil {
			return err
		}
		fmt.Println("See the Person struct: ", px.Name, " - ", px.Age)
		fmt.Println("See the Address struct: ", addr.Street, " - ", addr.City)

		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
			return err
		}
	}
}

```


## Contributing

Welcome to **gokfk-regent**! We appreciate your interest in contributing to this project. Whether you're an experienced developer or just getting started, there are several ways you can help improve and expand this small project.

### Tasks to Contribute

Here are some tasks that you can work on:

- **Implement TopicRecodNameStrategy for Protobuf, JsonSchema, Avro:**


License
=======

[Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)
