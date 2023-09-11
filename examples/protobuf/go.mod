module examples

go 1.21.0

replace github.com/djedjethai/kfk-schemaregistry => ../..

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.2.0
	github.com/djedjethai/kfk-schemaregistry v0.0.0-20230902093333-3f654336d78c
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/bufbuild/protocompile v0.4.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/jhump/protoreflect v1.15.1 // indirect
	golang.org/x/sync v0.1.0 // indirect
	google.golang.org/genproto v0.0.0-20230815205213-6bfd019c3878 // indirect
)
