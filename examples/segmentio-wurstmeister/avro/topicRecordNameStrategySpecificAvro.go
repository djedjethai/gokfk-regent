package main

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"os"
// 	// "reflect"
// 	"strings"
//
// 	avSch "avroexample/schemas"
// 	schemaregistry "github.com/djedjethai/gokfk-regent"
// 	"github.com/djedjethai/gokfk-regent/serde"
// 	"github.com/djedjethai/gokfk-regent/serde/avro"
// 	kafka "github.com/segmentio/kafka-go"
// 	"log"
// 	"time"
// )
//
// const (
// 	producerMode          string = "producer"
// 	consumerMode          string = "consumer"
// 	nullOffset                   = -1
// 	topic                        = "my-topic"
// 	secondTopic                  = "my-second"
// 	kafkaURL                     = "127.0.0.1:29092"
// 	srURL                        = "http://127.0.0.1:8081"
// 	schemaFile            string = "./api/v1/proto/Person.proto"
// 	consumerGroupID              = "test-consumer"
// 	defaultSessionTimeout        = 6000
// 	noTimeout                    = -1
// 	groupID                      = "logger-group"
// 	personFQN                    = "personrecord.Person"
// 	addressFQN                   = "addressrecord.Address"
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
// 	// msg := &avSch.Person{
// 	// 	Name: "robert",
// 	// 	Age:  23,
// 	// }
//
// 	addr := &avSch.Address{
// 		Street: "rue de la soif",
// 		City:   "Rennes",
// 	}
//
// 	// job := &avSch.Job{
// 	// 	Job:   "doctor",
// 	// 	Field: "medicin",
// 	// }
//
// 	for {
// 		// err := producer.ProduceMessage(msg, topic, personFQN)
// 		// if err != nil {
// 		// 	log.Println("Error producing Message: ", err)
// 		// }
//
// 		// err = producer.ProduceMessage(addr, topic, addressFQN)
// 		// if err != nil {
// 		// 	log.Println("Error producing Message: ", err)
// 		// }
//
// 		err = producer.ProduceMessage(addr, secondTopic, addressFQN)
// 		if err != nil {
// 			log.Println("Error producing Message: ", err)
// 		}
//
// 		// // job schema have no namespace, so gokfk-regent will use the Go fullyQualifiedName
// 		// // schemas.Job
// 		// err = producer.ProduceMessage(job, secondTopic)
// 		// if err != nil {
// 		// 	log.Println("Error producing Message: ", err)
// 		// }
//
// 		time.Sleep(2 * time.Second)
// 	}
// }
//
// // SRProducer interface
// type SRProducer interface {
// 	ProduceMessage(msg interface{}, topic string, subject ...string) error
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
// 	s, err := avro.NewSpecificSerializer(c, serde.ValueSerde, avro.NewSerializerConfig())
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
// func (p *srProducer) ProduceMessage(msg interface{}, topic string, subject ...string) error {
//
// 	var err error
// 	var payload []byte
// 	if len(subject) > 0 {
// 		sub := fmt.Sprintf("%s-%s", topic, subject[0])
// 		payload, err = p.serializer.SerializeTopicRecordName(topic, msg, sub)
// 	} else {
// 		// the subject is optional, it only assert the object fqn with the expected one
// 		payload, err = p.serializer.SerializeTopicRecordName(topic, msg)
// 	}
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
// 	err = consumer.Run()
// 	if err != nil {
// 		log.Println("ConsumerRun Error: ", err)
// 	}
// }
//
// // SRConsumer interface
// type SRConsumer interface {
// 	Run() error
// 	Close()
// }
//
// type srConsumer struct {
// 	reader       *kafka.Reader
// 	secondReader *kafka.Reader
// 	deserializer *avro.SpecificDeserializer
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
// 	d, err := avro.NewSpecificDeserializer(sr, serde.ValueSerde, avro.NewDeserializerConfig())
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
// func (c *srConsumer) RegisterMessageFactory() func([]string, string) (interface{}, error) {
// 	return func(subject []string, name string) (interface{}, error) {
// 		fmt.Println("AvroSpec topicRecNameStrat RegisterMessageFactory - subject: ", subject)
// 		fmt.Println("AvroSpec topicRecNameStrat RegisterMessageFactory - name: ", name)
// 		switch name {
// 		case fmt.Sprintf("my-topic-%s", personFQN):
// 			return &avSch.Person{}, nil
// 		case fmt.Sprintf("my-topic-%s", addressFQN):
// 			return &avSch.Address{}, nil
// 		case fmt.Sprintf("my-second-%s", addressFQN):
// 			return &avSch.Address{}, nil
// 		case "my-second-schemas.Job":
// 			// as Job schema have no namespace, gokfk-regent refere to the Go fqn
// 			return &avSch.Job{}, nil
// 		}
// 		return nil, errors.New("No matching receiver")
// 	}
// }
//
// // consumer
// func (c *srConsumer) Run() error {
//
// 	ctx := context.Background()
//
// 	// register the MessageFactory is optional
// 	c.deserializer.MessageFactory = c.RegisterMessageFactory()
//
// 	// case recordIntoTopicNameSTrategy
// 	var pxTopic = avSch.Person{}
// 	var addrTopic = avSch.Address{}
// 	var addrSecondTopic = avSch.Address{}
// 	msgFQN := fmt.Sprintf("%s-%s", topic, personFQN)
// 	addrFQNTopic := fmt.Sprintf("%s-%s", topic, addressFQN)
// 	addrFQNSecondTopic := fmt.Sprintf("%s-%s", secondTopic, addressFQN)
// 	ref := make(map[string]interface{})
// 	ref[msgFQN] = &pxTopic
// 	ref[addrFQNTopic] = &addrTopic
// 	ref[addrFQNSecondTopic] = &addrSecondTopic
// 	ref["my-second-schemas.Job"] = &avSch.Job{}
//
// 	fmt.Println("start consuming ... !!")
//
// 	msg := make(chan interface{})
//
// 	// go c.getResponseIntoTopicRecordName(ctx, msg, c.reader, ref)
// 	// go c.getResponseIntoTopicRecordName(ctx, msg, c.secondReader, ref)
//
// 	go c.getResponseTopicRecordName(ctx, msg, c.reader)
// 	go c.getResponseTopicRecordName(ctx, msg, c.secondReader)
//
// 	for {
// 		select {
// 		case value := <-msg:
// 			switch v := value.(type) {
// 			case error:
// 				fmt.Println("Received an error:", v)
// 			case kafka.Message:
// 				fmt.Printf("Kafka message at topic:%v partition:%v offset:%v	%s\n", v.Topic, v.Partition, v.Offset, string(v.Key))
// 			default:
// 				fmt.Println("Received a message:", v)
// 			}
// 		case <-ctx.Done():
// 			close(msg)
// 		}
// 	}
// }
//
// func (c *srConsumer) getResponseIntoTopicRecordName(ctx context.Context, res chan interface{}, reader *kafka.Reader, receiver map[string]interface{}) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return // Exit the loop when the context is canceled.
// 		default:
// 		}
//
// 		m, err := reader.ReadMessage(context.Background())
// 		if err != nil {
// 			res <- err
// 		}
//
// 		err = c.deserializer.DeserializeIntoTopicRecordName(m.Topic, receiver, m.Value)
// 		if err != nil {
// 			res <- err
// 		}
//
// 		res <- m
// 		res <- receiver
// 	}
// }
//
// func (c *srConsumer) getResponseTopicRecordName(ctx context.Context, res chan interface{}, reader *kafka.Reader) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return // Exit the loop when the context is canceled.
// 		default:
// 		}
//
// 		m, err := reader.ReadMessage(context.Background())
// 		if err != nil {
// 			res <- err
// 		}
//
// 		msg, err := c.deserializer.DeserializeTopicRecordName(m.Topic, m.Value)
// 		if err != nil {
// 			res <- err
// 		}
//
// 		res <- m
// 		res <- msg
// 	}
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
