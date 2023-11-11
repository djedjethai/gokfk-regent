module avroexample

go 1.21.0

replace github.com/djedjethai/gokfk-regent => ../../..

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/confluentinc/confluent-kafka-go/v2 v2.2.0
	github.com/djedjethai/gokfk-regent v0.0.0-00010101000000-000000000000
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/heetch/avro v0.4.4 // indirect
	github.com/linkedin/goavro v2.1.0+incompatible // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230913181813-007df8e322eb // indirect
)
