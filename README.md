# gokfk-regent is a Go Kafka-Schema-Registry-Client implementing all strategies TopicName, RecordName, and TopicRecordName. 

**gokfk-regent is a fork of Confluent's Golang client that introduces the Topic-Name-Strategy, Record-Name-Strategy, and Topic-Record-Name-Strategy implementations—features** currently absent in the original client. This fork caters to users with specific functionality requirements, enhancing their experience with Apache Kafka.

**gokfk-regent improves cache management in lrucache.go, an issue that remains open in the original project(at this day)**. This fork addresses the problem by implementing the necessary fix, ensuring the cache operates as intended by respecting its allocated capacity.

**gokfk-regent is a fork of confluent-kafka-go that harnesses the power of go-confluent-schemaregistry. As a nimble open-source project, it prioritizes agility, enabling swift development, and enthusiastically welcomes contributions from the community.**


## Implemented
* TopicNameStrategy for ProtoBuf, JsonSchema, Avro(from the parent project/confluent-serde)
* RecordNameStrategy for Protobuf, JsonSchema, Avro  


## Install

``` bash
$ go get https://github.com/djedjethai/gokfk-regent/v1
```

## Use the RecordNameStrategy with Protobuf
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
		offset, err := producer.ProduceMessage(msg, topic, subjectPerson)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		offset, err = producer.ProduceMessage(city, topic, subjectAddress)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}


/*
* ===============================
* CONSUMER
* ===============================
**/

var person = &pb.Person{}
var address = &pb.Address{}

func consumer() {
	consumer, err := NewConsumer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	personType := (&pb.Person{}).ProtoReflect().Type()
	addressType := (&pb.Address{}).ProtoReflect().Type()

	
	// Deserialize into a struct
	// works with DeserializeRecordName and DeserializeIntoRecordName
	subjects := make(map[string]interface{})
	subjects[subjectPerson] = person
	subjects[subjectAddress] = address

	err = consumer.Run([]protoreflect.MessageType{personType, addressType}, topic, subjects)
	if err != nil {
		log.Println("ConsumerRun Error: ", err)
	}

}

// Run consumer
func (c *srConsumer) Run(messagesType []protoreflect.MessageType, topic string, subjects map[string]interface{}) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}

	if len(messagesType) > 0 {
		for _, mt := range messagesType {
			if err := c.deserializer.ProtoRegistry.RegisterMessage(mt); err != nil {

				return err
			}
		}
	}

	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}

		msg, err := c.deserializer.DeserializeRecordName(subjects, kafkaMsg.Value)
		if err != nil {
			return err
		}
		c.handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))

		// // could instanciate a second map(or overwrite the previous one)
		// subjects := make(map[string]interface{})
		// person := &pb.Person{}
		// subjects[subjectPerson] = person
		// address := &pb.Address{}
		// subjects[subjectAddress] = address

		err = c.deserializer.DeserializeIntoRecordName(subjects, kafkaMsg.Value)
		if err != nil {
			return err
		}

		fmt.Println("person: ", person.Name, " - ", person.Age)
		fmt.Println("address: ", address.City, " - ", address.Street)

		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
			return err
		}
	}
}
``` 

## Use the RecordNameStrategy with JsonSchema
(see in ./examples for the full implementation)
```
/*
* ===============================
* PRODUCER
* ===============================
**/

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type Address struct {
	Street string `json:"street"`
	City   string `json:"city"`
}

type EmbeddedPax struct {
	Name    string  `json:"name"`
	Age     int     `json:"age"`
	Address Address `json:"address"`
}

type Embedded struct {
	Pax    EmbeddedPax `json:"pax"`
	Status string      `json:"status"`
}

func producer() {
	producer, err := NewProducer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	msg := Person{
		Name: "robert",
		Age:  23,
	}

	addr := Address{
		Street: "myStreet",
		City:   "Berlin",
	}

	px := EmbeddedPax{
		Name:    "robert",
		Age:     23,
		Address: addr,
	}

	embedded := Embedded{
		Pax:    px,
		Status: "embedded",
	}

	for {
		offset, err := producer.ProduceMessage(msg, topic)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		offset, err = producer.ProduceMessage(addr, topic)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		offset, err = producer.ProduceMessage(px, topic)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		offset, err = producer.ProduceMessage(embedded, topic)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}
/*
* ===============================
* CONSUMER
* ===============================
**/
func (c *srConsumer) RegisterMessageFactoryIntoRecordName(subjectTypes map[string]interface{}) func(string, string) (interface{}, error) {

	return func(subject string, name string) (interface{}, error) {

		// subject have the form: package.Type-value
		if v, ok := subjectTypes[strings.TrimSuffix(subject, "-value")]; !ok {
			return nil, errors.New("Invalid receiver")
		} else {
			return v, nil
		}
	}
}

func (c *srConsumer) RegisterMessageFactoryRecordName() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch strings.TrimSuffix(subject, "-value") {
		case "main.Person":
			return &Person{}, nil
		case "main.Address":
			return &Address{}, nil
		case "main.Embedded":
			return &Embedded{}, nil
		case "main.EmbeddedPax":
			return &EmbeddedPax{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}

// Run consumer
// func (c *srConsumer) Run(messageType protoreflect.MessageType, topic string) error {
func (c *srConsumer) Run(topic string) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}

	// case recordNameStrategy
	msgFQN := "main.Person"
	addrFQN := "main.Address"
	embPaxFQN := "main.EmbeddedPax"
	embFQN := "main.Embedded"
	ref := make(map[string]interface{})
	ref[msgFQN] = struct{}{}
	ref[addrFQN] = struct{}{}
	ref[embFQN] = struct{}{}
	ref[embPaxFQN] = struct{}{}
	c.deserializer.MessageFactory = c.RegisterMessageFactoryRecordName()

	// // case recordIntoNameSTrategy
	// px := Person{}
	// addr := Address{}
	// embPax := EmbeddedPax{}
	// emb := Embedded{}
	// msgFQN := reflect.TypeOf(px).String()
	// addrFQN := reflect.TypeOf(addr).String()
	// embPaxFQN := reflect.TypeOf(embPax).String()
	// embFQN := reflect.TypeOf(emb).String()
	// ref := make(map[string]interface{})
	// ref[msgFQN] = &px
	// ref[addrFQN] = &addr
	// ref[embPaxFQN] = &embPax
	// ref[embFQN] = &emb
	// c.deserializer.MessageFactory = c.RegisterMessageFactoryIntoRecordName(ref)

	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}

		// get a msg of type interface{}
		msg, err := c.deserializer.DeserializeRecordName(ref, kafkaMsg.Value)
		if err != nil {
			return err
		}
		c.handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))

		// // use deserializer.DeserializeIntoRecordName to get a struct back
		// err = c.deserializer.DeserializeIntoRecordName(ref, kafkaMsg.Value)
		// if err != nil {
		// 	return err
		// }
		// fmt.Println("message deserialized into: ", px.Name, " - ", addr.Street)
		// fmt.Println("message deserialized into EmbeddedPax: ", embPax.Name, " - ", embPax.Address.Street)
		// fmt.Println("message deserialized into Emb: ", emb.Pax.Name, " - ", emb.Pax.Address.Street, " - ", emb.Status)

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
