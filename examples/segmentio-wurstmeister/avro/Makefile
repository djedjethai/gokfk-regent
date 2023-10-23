SCHEMA_PATH = $(shell pwd)/schemas
SCHEMA_FILE = schemas.avsc
NONAMESPACE_FILE = nonamespace.avsc
SCHEMAS = schemas

.PHONY: compile
compile:
	gogen-avro --containers=false --sources-comment=false --short-unions=false --package=${SCHEMAS} ${SCHEMA_PATH} ${SCHEMA_FILE}

.PHONY: compile_noname
compile_noname:
	gogen-avro --containers=false --sources-comment=false --short-unions=false --package=${SCHEMAS} ${SCHEMA_PATH} ${NONAMESPACE_FILE}



