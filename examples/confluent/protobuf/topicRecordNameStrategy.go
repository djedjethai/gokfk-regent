package main

import (
	"context"
	"errors"
	pb "examples/api/v1/proto"
	"fmt"
	"os"
	"strings"

	// "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	schemaregistry "github.com/djedjethai/gokfk-regent"
	"github.com/djedjethai/gokfk-regent/serde"
	"github.com/djedjethai/gokfk-regent/serde/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"time"
)

const (
	producerMode             string = "producer"
	consumerMode             string = "consumer"
	nullOffset                      = -1
	topic                           = "my-topic"
	second                          = "second"
	kafkaURL                        = "127.0.0.1:29092"
	srURL                           = "http://127.0.0.1:8081"
	schemaFile               string = "./api/v1/proto/Person.proto"
	consumerGroupID                 = "test-consumer"
	defaultSessionTimeout           = 6000
	noTimeout                       = -1
	subjectPerson                   = "test.v1.Person"
	topicSubjectPerson              = "my-topic-test.v1.Person"
	secondSubjectPerson             = "second-test.v1.Person"
	topicSubjectPersonValue         = "my-topic-test.v1.Person-value"
	secondSubjectPersonValue        = "second-test.v1.Person-value"
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

	msg2 := &pb.Person{
		Name: "steve",
		Age:  59,
	}

	for {
		offset, err := producer.ProduceMessage(msg, topic, topicSubjectPerson)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		offset, err = producer.ProduceMessage(msg2, second, secondSubjectPerson)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}

// SRProducer interface
type SRProducer interface {
	ProduceMessage(msg proto.Message, topic, subject string) (int64, error)
	Close()
}

type srProducer struct {
	producer   *kafka.Producer
	serializer serde.Serializer
}

// NewProducer returns kafka producer with schema registry
func NewProducer(kafkaURL, srURL string) (SRProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
	if err != nil {
		return nil, err
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
		producer:   p,
		serializer: s,
	}, nil
}

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg proto.Message, topic, subject string) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)

	// convenient
	// payload, err := p.serializer.SerializeTopicRecordName(topic, msg)

	// assert the fullyQualifiedName. log an err if mismatch
	payload, err := p.serializer.SerializeTopicRecordName(topic, msg, subject)
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

// Close schema registry and Kafka
func (p *srProducer) Close() {
	p.serializer.Close()
	p.producer.Close()
}

/*
* ==============================
* CONSUMER
* ==============================
**/

var person = &pb.Person{}
var address = &pb.Address{}

func consumer() {

	consumer, err := NewConsumer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	personType := (&pb.Person{}).ProtoReflect().Type()

	err = consumer.Run([]protoreflect.MessageType{personType})
	if err != nil {
		log.Println("ConsumerRun Error: ", err)
	}

}

// SRConsumer interface
type SRConsumer interface {
	Run(messagesType []protoreflect.MessageType) error
	Close()
}

type srConsumer struct {
	topicConsumer  *kafka.Consumer
	secondConsumer *kafka.Consumer
	deserializer   *protobuf.Deserializer
}

// NewConsumer returns new consumer with schema registry
func NewConsumer(kafkaURL, srURL string) (SRConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaURL,
		"group.id":           consumerGroupID,
		"session.timeout.ms": defaultSessionTimeout,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	c2, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaURL,
		"group.id":           "secondConsumer",
		"session.timeout.ms": defaultSessionTimeout,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &srConsumer{
		topicConsumer:  c,
		secondConsumer: c2,
		deserializer:   d,
	}, nil
}

// RegisterMessageFactory will overwrite the default one
// In this case &pb.Person{} is the "msg" at "msg, err := c.deserializer.DeserializeRecordName()"
func (c *srConsumer) RegisterMessageFactory() func([]string, string) (interface{}, error) {
	return func(subjects []string, name string) (interface{}, error) {
		if len(subjects) > 0 {
			for _, subject := range subjects {
				switch subject {
				case topicSubjectPersonValue, secondSubjectPersonValue:
					return &pb.Person{}, nil
				}
			}
		}
		return nil, errors.New("No matching receiver")
	}
}

// Run consumer
func (c *srConsumer) Run(messagesType []protoreflect.MessageType) error {

	if err := c.topicConsumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}
	if err := c.secondConsumer.SubscribeTopics([]string{second}, nil); err != nil {
		return err
	}

	if len(messagesType) > 0 {
		for _, mt := range messagesType {
			if err := c.deserializer.ProtoRegistry.RegisterMessage(mt); err != nil {

				return err
			}
		}
	}

	// register the MessageFactory is facultatif
	// but is it useful to allow the event receiver to be an initialized object
	c.deserializer.MessageFactory = c.RegisterMessageFactory()

	// Deserialize into a struct
	// receivers for DeserializeIntoRecordName
	subjects := make(map[string]interface{})
	subjects2 := make(map[string]interface{})
	subjects[topicSubjectPersonValue] = &pb.Person{}

	// NOTE:!!!! Using 'subjects' as a single receiver for the 2 topics is a possibility,
	// but it has a caveat:
	// The Kafka schema registry will store both topicRecordName values with the same schema,
	// meaning that the *pb.Person's schema will only be registered once.
	// This schema will have multiple subjects, such as:
	// PROTOBUF [] [my-topic-test.v1.Person-value second-test.v1.Person-value]
	// The gokfk-regent loop on the subjects until it finds the matching one.
	// So, if 'subjects' is used for both goroutines,
	// then the deserialized object will always be subjects[my-topic-test.v1.Person-value],
	// as it always comes first.
	subjects2[secondSubjectPersonValue] = &pb.Person{}

	fmt.Println("start consuming ... !!")

	msg := make(chan interface{})
	ctx := context.Background()

	// go c.getResponseTopicRecordName(ctx, topic, msg)
	// go c.getResponseTopicRecordName(ctx, second, msg)
	go c.getResponseIntoTopicRecordName(ctx, topic, msg, subjects)
	go c.getResponseIntoTopicRecordName(ctx, second, msg, subjects2)

	for {
		select {
		case value := <-msg:
			switch v := value.(type) {
			case error:

				fmt.Println("Received an error:", v)
			case string:
				fmt.Println("Kafka message is: ", v)
			default:
				fmt.Println("default:", v)
			}
		case <-ctx.Done():
			close(msg)
		}
	}
}

func (c *srConsumer) getResponseTopicRecordName(ctx context.Context, chTopic string, res chan interface{}) {
	for {
		select {
		case <-ctx.Done():
			return // Exit the loop when the context is canceled.
		default:
		}

		var kafkaMsg *kafka.Message
		var err error
		if chTopic == topic {
			kafkaMsg, err = c.topicConsumer.ReadMessage(noTimeout)
			if err != nil {
				res <- err
				continue
			}
		} else if chTopic == second {
			kafkaMsg, err = c.secondConsumer.ReadMessage(noTimeout)
			if err != nil {
				res <- err
				continue
			}
		} else {
			res <- errors.New("unknown topic")
			continue
		}

		msg, err := c.deserializer.DeserializeTopicRecordName(topic, kafkaMsg.Value)
		if err != nil {
			res <- err
		}

		// without RegisterMessageFactory()
		// res <- c.handleMessageAsInterface(chTopic, msg, int64(kafkaMsg.TopicPartition.Offset))

		// with RegisterMessageFactory()
		if _, ok := msg.(*pb.Person); ok {
			res <- fmt.Sprintf("[TRN]topic: %v - person: %v %v", chTopic, msg.(*pb.Person).Name, msg.(*pb.Person).Age)
		}

		if chTopic == topic {
			if _, err = c.topicConsumer.CommitMessage(kafkaMsg); err != nil {
				res <- err
			}
		} else {
			if _, err = c.secondConsumer.CommitMessage(kafkaMsg); err != nil {
				res <- err
			}
		}
	}
}

func (c *srConsumer) getResponseIntoTopicRecordName(ctx context.Context, chTopic string, res chan interface{}, receiver map[string]interface{}) {
	for {
		select {
		case <-ctx.Done():
			return // Exit the loop when the context is canceled.
		default:
		}

		var kafkaMsg *kafka.Message
		var err error
		if chTopic == topic {
			kafkaMsg, err = c.topicConsumer.ReadMessage(noTimeout)
			if err != nil {
				res <- err
				continue
			}
		} else if chTopic == second {
			kafkaMsg, err = c.secondConsumer.ReadMessage(noTimeout)
			if err != nil {
				res <- err
				continue
			}
		} else {
			res <- errors.New("unknown topic")
			continue
		}

		err = c.deserializer.DeserializeIntoTopicRecordName(chTopic, receiver, kafkaMsg.Value)
		if err != nil {
			res <- err
		}

		if chTopic == topic {
			res <- fmt.Sprintf("[IntoTRN]topic: %v - person: %v %v", chTopic, receiver[topicSubjectPersonValue].(*pb.Person).Name, receiver[topicSubjectPersonValue].(*pb.Person).Age)
		}
		if chTopic == second {
			res <- fmt.Sprintf("[IntoTRN]topic: %v - person: %v %v", chTopic, receiver[secondSubjectPersonValue].(*pb.Person).Name, receiver[secondSubjectPersonValue].(*pb.Person).Age)
		}

		if chTopic == topic {
			if _, err = c.topicConsumer.CommitMessage(kafkaMsg); err != nil {
				res <- err
			}
		} else {
			if _, err = c.secondConsumer.CommitMessage(kafkaMsg); err != nil {
				res <- err
			}
		}

	}
}

func (c *srConsumer) handleMessageAsInterface(topic string, message interface{}, offset int64) string {
	return fmt.Sprintf("[HandleMsgAsInterface]topic: %v - message %v with offset %d\n", topic, message, offset)
}

// Close all connections
func (c *srConsumer) Close() {
	err := c.topicConsumer.Close()
	if err != nil {
		log.Println(err)
	}

	err = c.secondConsumer.Close()
	if err != nil {
		log.Println(err)
	}

	c.deserializer.Close()
}
