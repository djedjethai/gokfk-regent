package main

// import (
// 	"context"
// 	"errors"
// 	pb "examples/api/v1/proto"
// 	"fmt"
// 	"os"
// 	"strings"
//
// 	schemaregistry "github.com/djedjethai/gokfk-regent"
// 	"github.com/djedjethai/gokfk-regent/serde"
// 	"github.com/djedjethai/gokfk-regent/serde/protobuf"
// 	kafka "github.com/segmentio/kafka-go"
// 	"google.golang.org/protobuf/proto"
// 	"google.golang.org/protobuf/reflect/protoreflect"
// 	"log"
// 	"time"
// )
//
// const (
// 	producerMode                   string = "producer"
// 	consumerMode                   string = "consumer"
// 	nullOffset                            = -1
// 	topic                                 = "my-topic"
// 	secondTopic                           = "my-second"
// 	kafkaURL                              = "127.0.0.1:29092"
// 	srURL                                 = "http://127.0.0.1:8081"
// 	schemaFile                     string = "./api/v1/proto/Person.proto"
// 	consumerGroupID                       = "test-consumer"
// 	defaultSessionTimeout                 = 6000
// 	noTimeout                             = -1
// 	topicSubjectPerson                    = "my-topic-test.v1.Person"
// 	topicSubjectPersonValue               = "my-topic-test.v1.Person-value"
// 	topicSubjectAddress                   = "my-topic-another.v1.Address"
// 	topicSubjectAddressValue              = "my-topic-another.v1.Address-value"
// 	secondTopicSubjectAddress             = "my-second-another.v1.Address"
// 	secondTopicSubjectAddressValue        = "my-second-another.v1.Address-value"
// 	groupID                               = "logger-group"
// )
//
// func main() {
//
// 	clientMode := os.Args[1]
//
// 	if strings.Compare(clientMode, producerMode) == 0 {
// 		producer()
// 	} else if strings.Compare(clientMode, consumerMode) == 0 {
// 		consumer()
// 	} else {
// 		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
// 			producerMode, consumerMode)
// 	}
// }
//
// func producer() {
// 	producer, err := NewProducer(kafkaURL, srURL)
// 	if err != nil {
// 		log.Fatal("Can not create producer: ", err)
// 	}
//
// 	msg := &pb.Person{
// 		Name: "robert",
// 		Age:  23,
// 	}
//
// 	city := &pb.Address{
// 		Street: "myStreet",
// 		City:   "Bangkok",
// 	}
//
// 	// Job protobuf have no package name, so it will refer to Go fullyQualifiedName(proto.Job)
// 	job := &pb.Job{
// 		Job:   "doctor",
// 		Field: "medicin",
// 	}
//
// 	for {
// 		// return an err as the topic-fullyQualifiedName unmatch the topic
// 		// err := producer.ProduceMessage(msg, topic, "my-second-test.v1.Person")
// 		err := producer.ProduceMessage(msg, topic, topicSubjectPerson)
// 		if err != nil {
// 			log.Println("Error producing Message: ", err)
// 		}
//
// 		err = producer.ProduceMessage(city, topic, topicSubjectAddress)
// 		if err != nil {
// 			log.Println("Error producing Message: ", err)
// 		}
//
// 		err = producer.ProduceMessage(city, secondTopic, secondTopicSubjectAddress)
// 		if err != nil {
// 			log.Println("Error producing Message: ", err)
// 		}
//
// 		err = producer.ProduceMessage(job, secondTopic, "my-second-proto.Job")
// 		if err != nil {
// 			log.Println("Error producing Message: ", err)
// 		}
//
// 		time.Sleep(2 * time.Second)
// 	}
// }
//
// // SRProducer interface
// type SRProducer interface {
// 	ProduceMessage(msg proto.Message, topic, subject string) error
// 	Close()
// }
//
// type srProducer struct {
// 	writer       *kafka.Writer
// 	writerSecond *kafka.Writer
// 	serializer   serde.Serializer
// }
//
// // NewProducer returns kafka producer with schema registry
// func NewProducer(kafkaURL, srURL string) (SRProducer, error) {
//
// 	w := &kafka.Writer{
// 		Addr:     kafka.TCP(kafkaURL),
// 		Topic:    topic,
// 		Balancer: &kafka.LeastBytes{},
// 	}
//
// 	wSecond := &kafka.Writer{
// 		Addr:     kafka.TCP(kafkaURL),
// 		Topic:    secondTopic,
// 		Balancer: &kafka.LeastBytes{},
// 	}
//
// 	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
// 	if err != nil {
// 		return nil, err
// 	}
// 	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &srProducer{
// 		writer:       w,
// 		writerSecond: wSecond,
// 		serializer:   s,
// 	}, nil
// }
//
// var count int
//
// // ProduceMessage sends serialized message to kafka using schema registry
// func (p *srProducer) ProduceMessage(msg proto.Message, topic, subject string) error {
// 	// set the subject following the topic-RecordName format
// 	payload, err := p.serializer.SerializeTopicRecordName(topic, msg, subject)
// 	if err != nil {
// 		return err
// 	}
//
// 	key := fmt.Sprintf("Key-%d", count)
//
// 	kfkmsg := kafka.Message{
// 		Key:   []byte(key),
// 		Value: payload,
// 	}
//
// 	switch topic {
// 	case "my-topic":
// 		err = p.writer.WriteMessages(context.Background(), kfkmsg)
// 		if err != nil {
// 			fmt.Println(err)
// 			return err
// 		}
// 	case "my-second":
// 		err = p.writerSecond.WriteMessages(context.Background(), kfkmsg)
// 		if err != nil {
// 			fmt.Println(err)
// 			return err
// 		}
// 	}
//
// 	fmt.Println("count produced message", count)
// 	count++
//
// 	return nil
// }
//
// // Close schema registry and Kafka
// func (p *srProducer) Close() {
// 	p.serializer.Close()
// }
//
// /*
// * ===============================
// * CONSUMER
// * ===============================
// **/
//
// func consumer() {
// 	consumer, err := NewConsumer(kafkaURL, srURL)
// 	if err != nil {
// 		log.Fatal("Can not create producer: ", err)
// 	}
//
// 	personType := (&pb.Person{}).ProtoReflect().Type()
// 	addressType := (&pb.Address{}).ProtoReflect().Type()
// 	jobType := (&pb.Job{}).ProtoReflect().Type()
//
// 	err = consumer.Run([]protoreflect.MessageType{personType, addressType, jobType})
// 	if err != nil {
// 		log.Println("ConsumerRun Error: ", err)
// 	}
//
// }
//
// // SRConsumer interface
// type SRConsumer interface {
// 	Run(messagesType []protoreflect.MessageType) error
// 	Close()
// }
//
// type srConsumer struct {
// 	reader       *kafka.Reader
// 	secondReader *kafka.Reader
// 	deserializer *protobuf.Deserializer
// }
//
// func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
// 	log.Println("kafkaURL: ", kafkaURL)
// 	brokers := strings.Split(kafkaURL, ",")
// 	log.Println("kafkaBrokers: ", brokers)
// 	return kafka.NewReader(kafka.ReaderConfig{
// 		Brokers:  brokers,
// 		GroupID:  groupID,
// 		Topic:    topic,
// 		MinBytes: 10e3, // 10KB
// 		MaxBytes: 10e6, // 10MB
// 	})
// }
//
// // NewConsumer returns new consumer with schema registry
// func NewConsumer(kafkaURL, srURL string) (SRConsumer, error) {
//
// 	// consumer for topic
// 	rdr := getKafkaReader(kafkaURL, topic, groupID)
//
// 	// consumer for secondTopic
// 	secondRdr := getKafkaReader(kafkaURL, secondTopic, groupID)
//
// 	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &srConsumer{
// 		reader:       rdr,
// 		secondReader: secondRdr,
// 		deserializer: d,
// 	}, nil
// }
//
// // RegisterMessageFactory will overwrite the default one
// // In this case &pb.Person{} is the "msg" at "msg, err := c.deserializer.DeserializeRecordName()"
// func (c *srConsumer) RegisterMessageFactory() func([]string, string) (interface{}, error) {
// 	return func(subjects []string, name string) (interface{}, error) {
// 		fmt.Println("The subject: ", subjects) // topic-fullyQualifiedName-key/value
// 		fmt.Println("The name: ", name)        // fullyQualifiedName
// 		for _, subject := range subjects {
// 			switch subject {
// 			case fmt.Sprintf("%s-value", topicSubjectPerson):
// 				return &pb.Person{}, nil
// 			case fmt.Sprintf("%s-value", topicSubjectAddress):
// 				return &pb.Address{}, nil
// 			case fmt.Sprintf("%s-value", secondTopicSubjectAddress):
// 				return &pb.Address{}, nil
// 			case "my-second-proto.Job-value":
// 				return &pb.Job{}, nil
// 			}
// 		}
// 		return nil, errors.New("No matching receiver")
// 	}
// }
//
// func (c *srConsumer) consumeTopic(topic string, m kafka.Message) error {
// 	// msg, err := c.deserializer.DeserializeRecordName(m.Value)
// 	msg, err := c.deserializer.DeserializeTopicRecordName(topic, m.Value)
// 	if err != nil {
// 		return err
// 	}
//
// 	// with RegisterMessageFactory()
// 	if _, ok := msg.(*pb.Person); ok {
// 		fmt.Println("Person: ", msg.(*pb.Person).Name, " - ", msg.(*pb.Person).Age)
// 	}
// 	if _, ok := msg.(*pb.Address); ok {
// 		fmt.Println("Address: ", msg.(*pb.Address).City, " - ", msg.(*pb.Address).Street)
// 	}
// 	if _, ok := msg.(*pb.Job); ok {
// 		fmt.Println("Job: ", msg.(*pb.Job).Job, " - ", msg.(*pb.Job).Field)
// 	}
//
// 	// without RegisterMessageFactory()
// 	// c.handleMessageAsInterface(msg, int64(m.Offset))
//
// 	fmt.Printf("message at topic:%v partition:%v offset:%v	%s\n", m.Topic, m.Partition, m.Offset, string(m.Key))
//
// 	return nil
// }
//
// func (c *srConsumer) consumeTopicInto(topic string, m kafka.Message, receiver map[string]interface{}) error {
// 	// msg, err := c.deserializer.DeserializeRecordName(m.Value)
// 	err := c.deserializer.DeserializeIntoTopicRecordName(topic, receiver, m.Value)
// 	if err != nil {
// 		return err
// 	}
// 	if topic == "my-topic" {
// 		fmt.Println("person my-topic: ", receiver[topicSubjectPersonValue].(*pb.Person).Name, " - ", receiver[topicSubjectPersonValue].(*pb.Person).Age)
// 		fmt.Println("address my-topic: ", receiver[topicSubjectAddressValue].(*pb.Address).City, " - ", receiver[topicSubjectAddressValue].(*pb.Address).Street)
// 	}
// 	if topic == "my-second" {
// 		fmt.Println("address my-second: ", receiver[secondTopicSubjectAddressValue].(*pb.Address).City, " - ", receiver[secondTopicSubjectAddressValue].(*pb.Address).Street)
// 		fmt.Println("job my-second: ", receiver["my-second-proto.Job-value"].(*pb.Job).Job, " - ", receiver["my-second-proto.Job-value"].(*pb.Job).Field)
//
// 	}
// 	fmt.Printf("message at topic:%v partition:%v offset:%v	%s\n", m.Topic, m.Partition, m.Offset, string(m.Key))
// 	return nil
// }
//
// // consumer
// func (c *srConsumer) Run(messagesType []protoreflect.MessageType) error {
//
// 	if len(messagesType) > 0 {
// 		for _, mt := range messagesType {
// 			if err := c.deserializer.ProtoRegistry.RegisterMessage(mt); err != nil {
//
// 				return err
// 			}
// 		}
// 	}
//
// 	// case of DeserializeIntoTopicRecordName
// 	receiverTopic := make(map[string]interface{})
// 	receiverTopic[topicSubjectPersonValue] = &pb.Person{}
// 	receiverTopic[topicSubjectAddressValue] = &pb.Address{}
//
// 	receiverSecondTopic := make(map[string]interface{})
// 	receiverSecondTopic[secondTopicSubjectAddressValue] = &pb.Address{}
// 	receiverSecondTopic["my-second-proto.Job-value"] = &pb.Job{}
//
// 	// c.deserializer.MessageFactory = c.RegisterMessageFactory()
//
// 	fmt.Println("start consuming ... !!")
// 	for {
// 		m, err := c.reader.ReadMessage(context.Background())
// 		if err != nil {
// 			log.Fatalln(err)
// 		}
// 		if m.Topic == topic {
// 			// _ = c.consumeTopic(topic, m)
// 			_ = c.consumeTopicInto(topic, m, receiverTopic)
// 		}
//
// 		mSecond, err := c.secondReader.ReadMessage(context.Background())
// 		if err != nil {
// 			log.Fatalln(err)
// 		}
// 		if mSecond.Topic == secondTopic {
// 			// _ = c.consumeTopic(secondTopic, mSecond)
// 			_ = c.consumeTopicInto(secondTopic, mSecond, receiverSecondTopic)
// 		}
// 	}
//
// }
//
// func (c *srConsumer) handleMessageAsInterface(message interface{}, offset int64) {
// 	fmt.Printf("message %v with offset %d\n", message, offset)
// }
//
// // Close all connections
// func (c *srConsumer) Close() {
// 	if err := c.reader.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// 	c.deserializer.Close()
// }
