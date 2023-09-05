module jsonexample

go 1.21.0

replace github.com/djedjethai/kfk-schemaregistry => ../..

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.2.0
	github.com/djedjethai/kfk-schemaregistry v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.3
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/iancoleman/orderedmap v0.0.0-20190318233801-ac98e3ecb4b0 // indirect
	github.com/invopop/jsonschema v0.7.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v5 v5.3.1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230822172742-b8732ec3820d // indirect
)
