.PHONY: build
build:
	go build -o kfk .

.PHONY: compExample
compile:
	protoc ./examples/api/v1/proto/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

	protoc ./examples/studentTopicRecordNameStrategy/apiTopRecNameStrat/v1/student/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

	protoc ./examples/teacherTopicRecordNameStrategy/apiTopRecNameStrat/v1/teacher/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: compTestTRN
compTestTRN:
	protoc ./test/proto/topicrecordname/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.


