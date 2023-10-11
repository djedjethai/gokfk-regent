package main

import (
	"context"
	"errors"
	pb "examples/api/v1/proto"
	"fmt"
	"os"
	"strings"

	schemaregistry "github.com/djedjethai/gokfk-regent"
	"github.com/djedjethai/gokfk-regent/serde"
	"github.com/djedjethai/gokfk-regent/serde/protobuf"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"time"
)

const (
	producerMode          string = "producer"
	consumerMode          string = "consumer"
	nullOffset                   = -1
	topic                        = "my-topic"
	kafkaURL                     = "127.0.0.1:29092"
	srURL                        = "http://127.0.0.1:8081"
	schemaFile            string = "./api/v1/proto/Person.proto"
	consumerGroupID              = "test-consumer"
	defaultSessionTimeout        = 6000
	noTimeout                    = -1
	subjectPerson                = "test.v1.Person"
	subjectAddress               = "another.v1.Address"
)

func main() {

	clientMode := os.Args[1]

	if strings.Compare(clientMode, producerMode) == 0 {
		producer()
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		consumer()
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}
}

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
		_, err := producer.ProduceMessage(msg, topic, subjectPerson)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		_, err = producer.ProduceMessage(city, topic, subjectAddress)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		// log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}

// SRProducer interface
type SRProducer interface {
	ProduceMessage(msg proto.Message, topic, subject string) (string, error)
	Close()
}

type srProducer struct {
	writer     *kafka.Writer
	serializer serde.Serializer
}

// NewProducer returns kafka producer with schema registry
func NewProducer(kafkaURL, srURL string) (SRProducer, error) {

	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}
	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return nil, err
	}
	return &srProducer{
		writer:     w,
		serializer: s,
	}, nil
}

var count int

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg proto.Message, topic, subject string) (string, error) {
	// assert the fullyQualifiedName. log an err if mismatch
	payload, err := p.serializer.SerializeRecordName(msg, subject)
	if err != nil {
		return "", err
	}

	// for i := 0; ; i++ {
	// key := fmt.Sprintf("Key-%d", count)
	key := fmt.Sprintf("Key")

	// key := fmt.Sprintf("Key")
	kfkmsg := kafka.Message{
		Key:   []byte(key),
		Value: payload,
	}
	err = p.writer.WriteMessages(context.Background(), kfkmsg)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	// TODO make all of that better, should have the offset....
	fmt.Println("count produced message", count)

	count++

	// TODO should return the offset
	return key, nil
}

// Close schema registry and Kafka
func (p *srProducer) Close() {
	p.serializer.Close()
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

	err = consumer.Run([]protoreflect.MessageType{personType, addressType}, topic)
	if err != nil {
		log.Println("ConsumerRun Error: ", err)
	}

}

// SRConsumer interface
type SRConsumer interface {
	Run(messagesType []protoreflect.MessageType, topic string) error
	Close()
}

type srConsumer struct {
	reader       *kafka.Reader
	deserializer *protobuf.Deserializer
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	log.Println("kafkaURL: ", kafkaURL)
	brokers := strings.Split(kafkaURL, ",")
	log.Println("kafkaBrokers: ", brokers)
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

// NewConsumer returns new consumer with schema registry
func NewConsumer(kafkaURL, srURL string) (SRConsumer, error) {

	groupID := "logger-group"

	rdr := getKafkaReader(kafkaURL, topic, groupID)

	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &srConsumer{
		reader:       rdr,
		deserializer: d,
	}, nil
}

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

	fmt.Println("start consuming ... !!")
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

		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

}

func (c *srConsumer) handleMessageAsInterface(message interface{}, offset int64) {
	fmt.Printf("message %v with offset %d\n", message, offset)
}

// Close all connections
func (c *srConsumer) Close() {
	if err := c.reader.Close(); err != nil {
		log.Fatal(err)
	}
	c.deserializer.Close()
}
