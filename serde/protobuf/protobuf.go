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

package protobuf

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"

	schemaregistry "github.com/djedjethai/gokfk-regent"
	"github.com/djedjethai/gokfk-regent/confluent"
	"github.com/djedjethai/gokfk-regent/confluent/types"
	"github.com/djedjethai/gokfk-regent/serde"
	protoV1 "github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/genproto/googleapis/type/color"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/genproto/googleapis/type/fraction"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/genproto/googleapis/type/month"
	"google.golang.org/genproto/googleapis/type/postaladdress"
	"google.golang.org/genproto/googleapis/type/quaternion"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/apipb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/sourcecontextpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/typepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Serializer represents a Protobuf serializer
type Serializer struct {
	serde.BaseSerializer
}

// Deserializer represents a Protobuf deserializer
type Deserializer struct {
	serde.BaseDeserializer
	ProtoRegistry *protoregistry.Types
}

var _ serde.Serializer = new(Serializer)
var _ serde.Deserializer = new(Deserializer)

var builtInDeps = make(map[string]string)

func init() {
	builtins := map[string]protoreflect.FileDescriptor{
		"confluent/meta.proto":                 confluent.File_schemaregistry_confluent_meta_proto,
		"confluent/type/decimal.proto":         types.File_schemaregistry_confluent_type_decimal_proto,
		"google/type/calendar_period.proto":    calendarperiod.File_google_type_calendar_period_proto,
		"google/type/color.proto":              color.File_google_type_color_proto,
		"google/type/date.proto":               date.File_google_type_date_proto,
		"google/type/datetime.proto":           datetime.File_google_type_datetime_proto,
		"google/type/dayofweek.proto":          dayofweek.File_google_type_dayofweek_proto,
		"google/type/expr.proto":               expr.File_google_type_expr_proto,
		"google/type/fraction.proto":           fraction.File_google_type_fraction_proto,
		"google/type/latlng.proto":             latlng.File_google_type_latlng_proto,
		"google/type/money.proto":              money.File_google_type_money_proto,
		"google/type/month.proto":              month.File_google_type_month_proto,
		"google/type/postal_address.proto":     postaladdress.File_google_type_postal_address_proto,
		"google/type/quaternion.proto":         quaternion.File_google_type_quaternion_proto,
		"google/type/timeofday.proto":          timeofday.File_google_type_timeofday_proto,
		"google/protobuf/any.proto":            anypb.File_google_protobuf_any_proto,
		"google/protobuf/api.proto":            apipb.File_google_protobuf_api_proto,
		"google/protobuf/descriptor.proto":     descriptorpb.File_google_protobuf_descriptor_proto,
		"google/protobuf/duration.proto":       durationpb.File_google_protobuf_duration_proto,
		"google/protobuf/empty.proto":          emptypb.File_google_protobuf_empty_proto,
		"google/protobuf/field_mask.proto":     fieldmaskpb.File_google_protobuf_field_mask_proto,
		"google/protobuf/source_context.proto": sourcecontextpb.File_google_protobuf_source_context_proto,
		"google/protobuf/struct.proto":         structpb.File_google_protobuf_struct_proto,
		"google/protobuf/timestamp.proto":      timestamppb.File_google_protobuf_timestamp_proto,
		"google/protobuf/type.proto":           typepb.File_google_protobuf_type_proto,
		"google/protobuf/wrappers.proto":       wrapperspb.File_google_protobuf_wrappers_proto,
	}
	var fds []*descriptorpb.FileDescriptorProto
	for _, value := range builtins {
		fd := protodesc.ToFileDescriptorProto(value)
		fds = append(fds, fd)
	}
	fdMap, err := desc.CreateFileDescriptors(fds)
	if err != nil {
		log.Fatalf("Could not create fds")
	}
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	for key, value := range fdMap {
		var writer strings.Builder
		err = printer.PrintProtoFile(value, &writer)
		if err != nil {
			log.Fatalf("Could not print %s", key)
		}
		builtInDeps[key] = writer.String()
	}
}

// NewSerializer creates a Protobuf serializer for Protobuf-generated objects
func NewSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*Serializer, error) {
	s := &Serializer{}
	err := s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// ConfigureDeserializer configures the Protobuf deserializer
func (s *Deserializer) ConfigureDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *serde.DeserializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
	}
	s.Client = client
	s.Conf = conf
	s.SerdeType = serdeType
	s.SubjectNameStrategy = serde.TopicNameStrategy
	s.MessageFactory = s.protoMessageFactory
	s.ProtoRegistry = new(protoregistry.Types)
	return nil
}

// SerializeTopicRecordName serialize a protbuf data
func (s *Serializer) SerializeTopicRecordName(topic string, msg interface{}, subject ...string) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be a protobuf message")
	}

	messageDescriptor := protoMsg.ProtoReflect().Descriptor()

	fullName := string(messageDescriptor.FullName())

	// if protobuf does not define a package name, add the Go fullyQualifiedName.......
	partsMsg := strings.Split(fullName, ".")
	if len(partsMsg) < 2 {
		msgFQNGo := reflect.TypeOf(msg).String()
		fullName = strings.TrimLeft(msgFQNGo, "*")
	}

	// add the topic to the fullyQualifiedName
	fullName = fmt.Sprintf("%s-%s", topic, fullName)
	if len(subject) > 0 {
		if fullName != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, fullName, subject[0])
		}
	}

	autoRegister := s.Conf.AutoRegisterSchemas
	normalize := s.Conf.NormalizeSchemas
	fileDesc, deps, err := s.toProtobufSchema(protoMsg)
	if err != nil {
		return nil, err
	}
	metadata, err := s.resolveDependencies(fileDesc, deps, "", autoRegister, normalize)
	if err != nil {
		return nil, err
	}
	info := schemaregistry.SchemaInfo{
		Schema:     metadata.Schema,
		SchemaType: metadata.SchemaType,
		References: metadata.References,
	}

	id, fromSR, err := s.GetID(fullName, protoMsg, info)
	if err != nil {
		return nil, err
	}
	msgIndexBytes := toMessageIndexBytes(protoMsg.ProtoReflect().Descriptor())
	msgBytes, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, err
	}

	payload, err := s.WriteBytes(id, fromSR, append(msgIndexBytes, msgBytes...))
	if err != nil {
		return nil, err
	}
	return payload, nil

}

// SerializeRecordName serialize a protbuf data
func (s *Serializer) SerializeRecordName(msg interface{}, subject ...string) ([]byte, error) {

	if msg == nil {
		return nil, nil
	}
	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be a protobuf message")
	}

	messageDescriptor := protoMsg.ProtoReflect().Descriptor()

	fullName := string(messageDescriptor.FullName())

	// if protobuf does not define a package name, add the Go fullyQualifiedName.......
	partsMsg := strings.Split(fullName, ".")
	if len(partsMsg) < 2 {
		msgFQNGo := reflect.TypeOf(msg).String()
		fullName = strings.TrimLeft(msgFQNGo, "*")
	}

	if len(subject) > 0 {
		if fullName != subject[0] {
			return nil, fmt.Errorf(`the payload's fullyQualifiedName: '%v' does not match the subject: '%v'`, fullName, subject[0])
		}
	}

	autoRegister := s.Conf.AutoRegisterSchemas
	normalize := s.Conf.NormalizeSchemas
	fileDesc, deps, err := s.toProtobufSchema(protoMsg)
	if err != nil {
		return nil, err
	}
	metadata, err := s.resolveDependencies(fileDesc, deps, "", autoRegister, normalize)
	if err != nil {
		return nil, err
	}
	info := schemaregistry.SchemaInfo{
		Schema:     metadata.Schema,
		SchemaType: metadata.SchemaType,
		References: metadata.References,
	}

	id, fromSR, err := s.GetID(fullName, protoMsg, info)
	if err != nil {
		return nil, err
	}
	msgIndexBytes := toMessageIndexBytes(protoMsg.ProtoReflect().Descriptor())
	msgBytes, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, err
	}

	payload, err := s.WriteBytes(id, fromSR, append(msgIndexBytes, msgBytes...))
	if err != nil {
		return nil, err
	}
	return payload, nil

}

// Serialize implements serialization of Protobuf data
func (s *Serializer) Serialize(topic string, msg interface{}) ([]byte, error) {

	if msg == nil {
		return nil, nil
	}
	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be a protobuf message. Got '%v'", t)
	}

	autoRegister := s.Conf.AutoRegisterSchemas
	normalize := s.Conf.NormalizeSchemas
	fileDesc, deps, err := s.toProtobufSchema(protoMsg)
	if err != nil {
		return nil, err
	}

	metadata, err := s.resolveDependencies(fileDesc, deps, "", autoRegister, normalize)
	if err != nil {
		return nil, err
	}

	info := schemaregistry.SchemaInfo{
		Schema:     metadata.Schema,
		SchemaType: metadata.SchemaType,
		References: metadata.References,
	}

	id, fromSR, err := s.GetID(topic, protoMsg, info)
	if err != nil {
		return nil, err
	}
	msgIndexBytes := toMessageIndexBytes(protoMsg.ProtoReflect().Descriptor())
	msgBytes, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, err
	}

	payload, err := s.WriteBytes(id, fromSR, append(msgIndexBytes, msgBytes...))
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *Serializer) toProtobufSchema(msg proto.Message) (*desc.FileDescriptor, map[string]string, error) {
	messageDesc, err := desc.LoadMessageDescriptorForMessage(protoV1.MessageV1(msg))
	if err != nil {
		return nil, nil, err
	}
	fileDesc := messageDesc.GetFile()
	deps := make(map[string]string)
	err = s.toDependencies(fileDesc, deps)
	if err != nil {
		return nil, nil, err
	}
	return fileDesc, deps, nil
}

func (s *Serializer) toDependencies(fileDesc *desc.FileDescriptor, deps map[string]string) error {
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	var writer strings.Builder
	err := printer.PrintProtoFile(fileDesc, &writer)
	if err != nil {
		return err
	}
	deps[fileDesc.GetName()] = writer.String()
	for _, d := range fileDesc.GetDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		err = s.toDependencies(d, deps)
		if err != nil {
			return err
		}
	}
	for _, d := range fileDesc.GetPublicDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		err = s.toDependencies(d, deps)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Serializer) resolveDependencies(fileDesc *desc.FileDescriptor, deps map[string]string, subject string, autoRegister bool, normalize bool) (schemaregistry.SchemaMetadata, error) {
	refs := make([]schemaregistry.Reference, 0, len(fileDesc.GetDependencies())+len(fileDesc.GetPublicDependencies()))
	for _, d := range fileDesc.GetDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		ref, err := s.resolveDependencies(d, deps, d.GetName(), autoRegister, normalize)
		if err != nil {
			return schemaregistry.SchemaMetadata{}, err
		}
		refs = append(refs, schemaregistry.Reference{d.GetName(), ref.Subject, ref.Version})
	}
	for _, d := range fileDesc.GetPublicDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		ref, err := s.resolveDependencies(d, deps, d.GetName(), autoRegister, normalize)
		if err != nil {
			return schemaregistry.SchemaMetadata{}, err
		}
		refs = append(refs, schemaregistry.Reference{d.GetName(), ref.Subject, ref.Version})
	}
	info := schemaregistry.SchemaInfo{
		Schema:     deps[fileDesc.GetName()],
		SchemaType: "PROTOBUF",
		References: refs,
	}
	var id = -1
	var err error
	var version = 0
	if subject != "" {
		if autoRegister {
			id, _, err = s.Client.Register(subject, info, normalize)
			if err != nil {
				return schemaregistry.SchemaMetadata{}, err
			}
		} else {
			id, err = s.Client.GetID(subject, info, normalize)
			if err != nil {
				return schemaregistry.SchemaMetadata{}, err
			}
		}
		version, err = s.Client.GetVersion(subject, info, normalize)
		if err != nil {
			return schemaregistry.SchemaMetadata{}, err
		}
	}
	metadata := schemaregistry.SchemaMetadata{
		SchemaInfo: info,
		ID:         id,
		Subject:    subject,
		Version:    version,
	}
	return metadata, nil
}

func toMessageIndexBytes(descriptor protoreflect.Descriptor) []byte {
	if descriptor.Index() == 0 {
		switch descriptor.Parent().(type) {
		case protoreflect.FileDescriptor:
			// This is an optimization for the first message in the schema
			return []byte{0}
		}
	}
	msgIndexes := toMessageIndexes(descriptor, 0)
	buf := make([]byte, (1+len(msgIndexes))*binary.MaxVarintLen64)
	length := binary.PutVarint(buf, int64(len(msgIndexes)))

	for _, element := range msgIndexes {
		length += binary.PutVarint(buf[length:], int64(element))
	}
	return buf[0:length]
}

// Adapted from ideasculptor, see https://github.com/riferrei/srclient/issues/17
func toMessageIndexes(descriptor protoreflect.Descriptor, count int) []int {
	index := descriptor.Index()
	switch v := descriptor.Parent().(type) {
	case protoreflect.FileDescriptor:
		// parent is FileDescriptor, we reached the top of the stack, so we are
		// done. Allocate an array large enough to hold count+1 entries and
		// populate first value with index
		msgIndexes := make([]int, count+1)
		msgIndexes[0] = index
		return msgIndexes[0:1]
	default:
		// parent is another MessageDescriptor.  We were nested so get that
		// descriptor's indexes and append the index of this one
		msgIndexes := toMessageIndexes(v, count+1)
		return append(msgIndexes, index)
	}
}

func ignoreFile(name string) bool {
	return strings.HasPrefix(name, "confluent/") ||
		strings.HasPrefix(name, "google/protobuf/") ||
		strings.HasPrefix(name, "google/type/")
}

// NewDeserializer creates a Protobuf deserializer for Protobuf-generated objects
func NewDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*Deserializer, error) {
	s := &Deserializer{}
	err := s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize deserialize events with subjects register with the TopicNameStrategy
func (s *Deserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	bytesRead, messageDesc, _, err := s.setMessageDescriptor(topic, payload)
	if err != nil {
		return nil, err
	}

	subject, err := s.SubjectNameStrategy(topic, s.SerdeType)
	if err != nil {
		return nil, err
	}

	var subjects []string
	subjects = append(subjects, subject)
	msg, err := s.MessageFactory(subjects, messageDesc.GetFullyQualifiedName())
	if err != nil {
		return nil, err
	}
	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be a protobuf message")
	}

	err = proto.Unmarshal(payload[6+bytesRead:], protoMsg)
	return protoMsg, err
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

// DeserializeTopicRecordName deserialize events register with the TopicRecordNameStrategy
func (s *Deserializer) DeserializeTopicRecordName(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}

	bytesRead, messageDesc, info, err := s.setMessageDescriptor("", payload)
	if err != nil {
		fmt.Println("protobuf.go - DeserializeTopicRecordName - err: ", err)
		return nil, err
	}

	msgFullyQlfName := messageDesc.GetFullyQualifiedName()

	topicMsgFullyQlfNameValue, err := s.SubjectNameStrategy(topic, s.SerdeType, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	var subjects []string
	partsMsg := strings.Split(msgFullyQlfName, ".")
	if len(partsMsg) > 1 {
		// protobuf have a declared packagename
		for _, s := range info.Subject {
			if topicMsgFullyQlfNameValue == s {
				subjects = append(subjects, s)
				break
			}
		}
		// no match subject found
		if len(subjects) == 0 {
			// retry with updating the cache
			_, err = s.retryGetSubjects(payload, subjects, topicMsgFullyQlfNameValue)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// protobuf packagename is undefined
		// update the cache in any case and return all subjects
		subjects, err = s.retryGetSubjects(payload, subjects, topicMsgFullyQlfNameValue)
		if err != nil {
			return nil, err
		}
	}

	if len(subjects) == 0 {
		return nil, fmt.Errorf("no subject found for: %v", topicMsgFullyQlfNameValue)
	}

	msg, err := s.MessageFactory(subjects, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be a protobuf message")
	}

	err = proto.Unmarshal(payload[6+bytesRead:], protoMsg)
	return protoMsg, err
}

// DeserializeRecordName deserialize events register with the RecordNameStrategy
func (s *Deserializer) DeserializeRecordName(payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}

	bytesRead, messageDesc, info, err := s.setMessageDescriptor("", payload)
	if err != nil {
		return nil, err
	}

	msgFullyQlfName := messageDesc.GetFullyQualifiedName()

	msgFullyQlfNameValue, err := s.SubjectNameStrategy("", s.SerdeType, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	infSub := info.Subject
	var subjects []string
	partsMsg := strings.Split(msgFullyQlfName, ".")
	if len(partsMsg) > 1 {
		// case packagename is defined
		for _, s := range infSub {
			if s == msgFullyQlfNameValue {
				subjects = append(subjects, s)
				break
			}
		}
		// no match subject found
		if len(subjects) == 0 {
			// retry with updating the cache
			_, err = s.retryGetSubjects(payload, subjects, msgFullyQlfNameValue)
			if err != nil {
				return nil, err
			}
		}

	} else {
		// packagename is not defined, assert it is not possible, return all
		// NOTE the downside: no cache refresh
		subjects = infSub
	}

	if len(subjects) == 0 {
		return nil, fmt.Errorf("no subject found for: %v", msgFullyQlfNameValue)
	}

	msg, err := s.MessageFactory(subjects, msgFullyQlfName)
	if err != nil {
		return nil, err
	}

	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be a protobuf message")
	}

	err = proto.Unmarshal(payload[6+bytesRead:], protoMsg)
	return protoMsg, err
}

func (s *Deserializer) setMessageDescriptor(subject string, payload []byte) (int, *desc.MessageDescriptor, schemaregistry.SchemaInfo, error) {

	var info = schemaregistry.SchemaInfo{}
	info, err := s.GetSchema(subject, payload)
	if err != nil {
		return 0, nil, info, err
	}

	fd, err := s.toFileDesc(info)
	if err != nil {
		return 0, nil, info, err
	}
	bytesRead, msgIndexes, err := readMessageIndexes(payload[6:])
	if err != nil {
		return 0, nil, info, err
	}
	messageDesc, err := toMessageDesc(fd, msgIndexes)
	if err != nil {
		return 0, nil, info, err
	}

	return bytesRead, messageDesc, info, nil
}

// DeserializeInto implements deserialization of Protobuf data to the given object
func (s *Deserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}

	bytesRead, messageDesc, _, err := s.setMessageDescriptor(topic, payload)
	if err != nil {
		return err
	}

	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return fmt.Errorf("deserialization target must be a protobuf message")
	}

	protoInfo := reflect.TypeOf(protoMsg).Elem()
	if protoInfo.Name() != messageDesc.GetName() {
		return fmt.Errorf("recipient proto object differs from incoming events")
	}

	return proto.Unmarshal(payload[6+bytesRead:], protoMsg)
}

// DeserializeIntoTopicRecordName deserialize bytes with topicRecordNameStrategy
func (s *Deserializer) DeserializeIntoTopicRecordName(topic string, subjects map[string]interface{}, payload []byte) error {
	if payload == nil {
		return nil
	}

	bytesRead, messageDesc, info, err := s.setMessageDescriptor("", payload)
	if err != nil {
		return err
	}

	msgFullyQlfName := messageDesc.GetFullyQualifiedName()

	topicMsgFullyQlfNameValue, err := s.SubjectNameStrategy(topic, s.SerdeType, msgFullyQlfName)
	if err != nil {
		return err
	}

	// handle subject
	var sub []string
	partsMsg := strings.Split(msgFullyQlfName, ".")
	if len(partsMsg) > 1 {
		// protobuf have a declared packagename
		for _, s := range info.Subject {
			if topicMsgFullyQlfNameValue == s {
				sub = append(sub, s)
				break
			}
		}
		// no match subject found
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
	} else {
		// protobuf package is undefined
		// update the cache in any case and return all subjects
		sub, err = s.retryGetSubjects(payload, sub, topicMsgFullyQlfNameValue)
		if err != nil {
			return err
		}
	}

	// deserialize payload
	lenSubjects := len(sub)
	if lenSubjects > 0 {
		for i, subject := range sub {
			err = deserializeInto(messageDesc, payload, subjects, bytesRead, subject)
			if err == nil {
				return err
			}

			if lenSubjects == i+1 && err == nil {
				err = fmt.Errorf("unfound subject declaration")
			}
		}
	} else {
		return fmt.Errorf("unfound subject declaration")
	}
	return err
}

func deserializeInto(messageDesc *desc.MessageDescriptor, payload []byte, subjects map[string]interface{}, bytesRead int, key string) error {

	if msg, ok := subjects[key]; ok {
		var protoMsg proto.Message
		switch t := msg.(type) {
		case proto.Message:
			protoMsg = t
		default:
			return fmt.Errorf("deserialization target must be a protobuf message")
		}

		protoInfo := reflect.TypeOf(protoMsg).Elem()
		if protoInfo.Name() != messageDesc.GetName() {
			return fmt.Errorf("recipient proto object differs from incoming events")
		}

		return proto.Unmarshal(payload[6+bytesRead:], protoMsg)
	} else {
		return fmt.Errorf("unfound subject declaration")
	}
}

// DeserializeIntoRecordName deserialize bytes with recordNameStrategy to some given objects
func (s *Deserializer) DeserializeIntoRecordName(subjects map[string]interface{}, payload []byte) error {
	if payload == nil {
		return nil
	}

	bytesRead, messageDesc, info, err := s.setMessageDescriptor("", payload)
	if err != nil {
		return err
	}

	msgFullyQlfName := messageDesc.GetFullyQualifiedName()

	msgFullyQlfNameValue, err := s.SubjectNameStrategy("", s.SerdeType, msgFullyQlfName)
	if err != nil {
		return err
	}

	// handle subject
	infSub := info.Subject
	var sub []string
	partsMsg := strings.Split(msgFullyQlfName, ".")
	if len(partsMsg) > 1 {
		for _, s := range infSub {
			if s == msgFullyQlfNameValue {
				sub = append(sub, s)
				break
			}
		}
		// no match subject found
		if len(sub) == 0 {
			// retry with updating the cache
			_, err = s.retryGetSubjects(payload, sub, msgFullyQlfNameValue)
			if err != nil {
				return err
			}
		}
	} else {
		// NOTE the downside: no cache refresh
		sub = infSub
	}

	// deserialize payload
	lenSubjects := len(sub)
	if lenSubjects > 0 {
		for i, subject := range sub {
			err = deserializeInto(messageDesc, payload, subjects, bytesRead, subject)
			if err == nil {
				return err
			}

			if lenSubjects == i+1 && err == nil {
				err = fmt.Errorf("unfound subject declaration")
			}
		}
	} else {
		return fmt.Errorf("unfound subject declaration")
	}
	return err
}

func (s *Deserializer) toFileDesc(info schemaregistry.SchemaInfo) (*desc.FileDescriptor, error) {
	deps := make(map[string]string)
	err := serde.ResolveReferences(s.Client, info, deps)
	if err != nil {
		return nil, err
	}
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			var schema string
			if filename == "." {
				schema = info.Schema
			} else {
				schema = deps[filename]
			}
			if schema == "" {
				schema = builtInDeps[filename]
			}
			return io.NopCloser(strings.NewReader(schema)), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(".")
	if err != nil {
		return nil, err
	}

	if len(fileDescriptors) != 1 {
		return nil, fmt.Errorf("could not resolve schema")
	}
	return fileDescriptors[0], nil
}

func readMessageIndexes(payload []byte) (int, []int, error) {
	arrayLen, bytesRead := binary.Varint(payload)
	if bytesRead <= 0 {
		return bytesRead, nil, fmt.Errorf("unable to read message indexes")
	}
	if arrayLen == 0 {
		// Handle the optimization for the first message in the schema
		return bytesRead, []int{0}, nil
	}
	msgIndexes := make([]int, arrayLen)
	for i := 0; i < int(arrayLen); i++ {
		idx, read := binary.Varint(payload[bytesRead:])
		if read <= 0 {
			return bytesRead, nil, fmt.Errorf("unable to read message indexes")
		}
		bytesRead += read
		msgIndexes[i] = int(idx)
	}
	return bytesRead, msgIndexes, nil
}

func toMessageDesc(descriptor desc.Descriptor, msgIndexes []int) (*desc.MessageDescriptor, error) {
	index := msgIndexes[0]

	switch v := descriptor.(type) {
	case *desc.FileDescriptor:
		if len(msgIndexes) == 1 {
			return v.GetMessageTypes()[index], nil
		}
		return toMessageDesc(v.GetMessageTypes()[index], msgIndexes[1:])
	case *desc.MessageDescriptor:
		if len(msgIndexes) == 1 {
			return v.GetNestedMessageTypes()[index], nil
		}
		return toMessageDesc(v.GetNestedMessageTypes()[index], msgIndexes[1:])
	default:
		return nil, fmt.Errorf("unexpected type")
	}
}

func (s *Deserializer) protoMessageFactory(subject []string, name string) (interface{}, error) {
	mt, err := s.ProtoRegistry.FindMessageByName(protoreflect.FullName(name))
	if mt == nil {
		err = fmt.Errorf("unable to find MessageType %s", name)
	}
	if err != nil {
		return nil, err
	}

	msg := mt.New()
	return msg.Interface(), nil
}
