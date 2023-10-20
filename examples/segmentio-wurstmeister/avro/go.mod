module avroexample

go 1.21.0

replace github.com/djedjethai/gokfk-regent => ../../..

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/djedjethai/gokfk-regent v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.44
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/heetch/avro v0.4.4 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/linkedin/goavro v2.1.0+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)
