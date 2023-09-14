SCHEMA_PATH = $(shell pwd)/schema/person
SCHEMA_FILE = person.avsc
PERSON = person

.PHONY: compile
compile:
	gogen-avro --containers=false --sources-comment=false --short-unions=false --package=${PERSON} ${SCHEMA_PATH} ${SCHEMA_FILE}
