// Code generated by github.com/actgardner/gogen-avro/v10. DO NOT EDIT.
package schemas

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/vm"
	"github.com/actgardner/gogen-avro/v10/vm/types"
)

type UnionPersonAddressTypeEnum int

const (
	UnionPersonAddressTypeEnumPerson UnionPersonAddressTypeEnum = 0

	UnionPersonAddressTypeEnumAddress UnionPersonAddressTypeEnum = 1
)

type UnionPersonAddress struct {
	Person    Person
	Address   Address
	UnionType UnionPersonAddressTypeEnum
}

func writeUnionPersonAddress(r UnionPersonAddress, w io.Writer) error {

	err := vm.WriteLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType {
	case UnionPersonAddressTypeEnumPerson:
		return writePerson(r.Person, w)
	case UnionPersonAddressTypeEnumAddress:
		return writeAddress(r.Address, w)
	}
	return fmt.Errorf("invalid value for UnionPersonAddress")
}

func NewUnionPersonAddress() UnionPersonAddress {
	return UnionPersonAddress{}
}

func (r UnionPersonAddress) Serialize(w io.Writer) error {
	return writeUnionPersonAddress(r, w)
}

func DeserializeUnionPersonAddress(r io.Reader) (UnionPersonAddress, error) {
	t := NewUnionPersonAddress()
	deser, err := compiler.CompileSchemaBytes([]byte(t.Schema()), []byte(t.Schema()))
	if err != nil {
		return t, err
	}

	err = vm.Eval(r, deser, &t)

	if err != nil {
		return t, err
	}
	return t, err
}

func DeserializeUnionPersonAddressFromSchema(r io.Reader, schema string) (UnionPersonAddress, error) {
	t := NewUnionPersonAddress()
	deser, err := compiler.CompileSchemaBytes([]byte(schema), []byte(t.Schema()))
	if err != nil {
		return t, err
	}

	err = vm.Eval(r, deser, &t)

	if err != nil {
		return t, err
	}
	return t, err
}

func (r UnionPersonAddress) Schema() string {
	return "[{\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Person\",\"namespace\":\"personrecord\",\"type\":\"record\"},{\"fields\":[{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"}],\"name\":\"Address\",\"namespace\":\"addressrecord\",\"type\":\"record\"}]"
}

func (_ UnionPersonAddress) SetBoolean(v bool)   { panic("Unsupported operation") }
func (_ UnionPersonAddress) SetInt(v int32)      { panic("Unsupported operation") }
func (_ UnionPersonAddress) SetFloat(v float32)  { panic("Unsupported operation") }
func (_ UnionPersonAddress) SetDouble(v float64) { panic("Unsupported operation") }
func (_ UnionPersonAddress) SetBytes(v []byte)   { panic("Unsupported operation") }
func (_ UnionPersonAddress) SetString(v string)  { panic("Unsupported operation") }

func (r *UnionPersonAddress) SetLong(v int64) {

	r.UnionType = (UnionPersonAddressTypeEnum)(v)
}

func (r *UnionPersonAddress) Get(i int) types.Field {

	switch i {
	case 0:
		r.Person = NewPerson()
		return &types.Record{Target: (&r.Person)}
	case 1:
		r.Address = NewAddress()
		return &types.Record{Target: (&r.Address)}
	}
	panic("Unknown field index")
}
func (_ UnionPersonAddress) NullField(i int)                  { panic("Unsupported operation") }
func (_ UnionPersonAddress) HintSize(i int)                   { panic("Unsupported operation") }
func (_ UnionPersonAddress) SetDefault(i int)                 { panic("Unsupported operation") }
func (_ UnionPersonAddress) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ UnionPersonAddress) AppendArray() types.Field         { panic("Unsupported operation") }
func (_ UnionPersonAddress) Finalize()                        {}

func (r UnionPersonAddress) MarshalJSON() ([]byte, error) {

	switch r.UnionType {
	case UnionPersonAddressTypeEnumPerson:
		return json.Marshal(map[string]interface{}{"personrecord.Person": r.Person})
	case UnionPersonAddressTypeEnumAddress:
		return json.Marshal(map[string]interface{}{"addressrecord.Address": r.Address})
	}
	return nil, fmt.Errorf("invalid value for UnionPersonAddress")
}

func (r *UnionPersonAddress) UnmarshalJSON(data []byte) error {

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if len(fields) > 1 {
		return fmt.Errorf("more than one type supplied for union")
	}
	if value, ok := fields["personrecord.Person"]; ok {
		r.UnionType = 0
		return json.Unmarshal([]byte(value), &r.Person)
	}
	if value, ok := fields["addressrecord.Address"]; ok {
		r.UnionType = 1
		return json.Unmarshal([]byte(value), &r.Address)
	}
	return fmt.Errorf("invalid value for UnionPersonAddress")
}
