package horm

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/challenai/horm/thrift/hbase"
	"github.com/challenai/horm/utils"
)

const HBaseTagHint = "hbase"

// base model for every hbase model
type Model struct {
	Rowkey string
}

// DB represent a HBase database
type DB struct {
	Error        error
	RowsAffected int64
	db           *hbase.THBaseServiceClient
	schemas      map[string]schema
}

// schema used to store struct field and column mapping information
type schema struct {
	col2field map[string]int
	field2col []string
}

// Model should implement Table interface to specify the namespace and table name.
type Table interface {
	Namespace() string
	TableName() string
}

// create a new hbase database from thrift client
func NewDB(client *hbase.THBaseServiceClient) *DB {
	hb := &DB{
		db:      client,
		schemas: map[string]schema{},
	}
	return hb
}

// HBase rows range query
func (h *DB) Find() *DB {
	return h
}

// parse imported model so that we don't need to parse all the model fields everytime.
func (h *DB) parseModel(model interface{}) schema {
	schm := schema{}
	ptrVals := reflect.ValueOf(model)
	if ptrVals.Kind() != reflect.Ptr {
		panic("can't parse model type bacause model is not a pointer")
	}
	vals := ptrVals.Elem()
	h.schemas[reflect.TypeOf(model).Name()] = schm
	schm.field2col = make([]string, vals.NumField())
	schm.col2field = map[string]int{}
	for i := 0; i < vals.Type().NumField(); i++ {
		if vals.Type().Field(i).Name == "Model" {
			continue
		}
		tagsList := strings.Split(vals.Type().Field(i).Tag.Get(HBaseTagHint), ",")
		if len(tagsList) < 2 {
			panic("hbase column doesn't have column family or qualifier")
		}
		name := strings.Join(tagsList[:2], ":")
		schm.field2col[i] = name
		schm.col2field[name] = i
	}
	return schm
}

// get a single row.
func (h *DB) Get(ctx context.Context, model interface{}, rowkey string) *DB {
	// border case: input a nil as model, not allowed
	if model == nil {
		panic("can't input nil as a model")
	}
	tb, ok := model.(Table)
	if !ok {
		panic("please set namespace and table name for this model")
	}

	schm, ok := h.schemas[reflect.TypeOf(model).Name()]
	if !ok {
		schm = h.parseModel(model)
	}

	result, err := h.db.Get(ctx, []byte(fmt.Sprintf("%s:%s", tb.Namespace(), tb.TableName())), &hbase.TGet{Row: []byte(rowkey)})
	if err != nil {
		h.Error = err
		return h
	}

	fields := reflect.ValueOf(model).Elem()
	for _, v := range result.ColumnValues {
		key := fmt.Sprintf("%s:%s", v.Family, v.Qualifier)
		if idx, ok := schm.col2field[key]; ok {
			field := fields.Field(idx)
			switch field.Kind() {
			case reflect.Int:
				field.Set(reflect.ValueOf(utils.DecodeInt(v.GetValue())))
			case reflect.String:
				field.Set(reflect.ValueOf(string(v.GetValue())))
			}
		}
	}
	return h
}

// insert or update model to HBase
func (h *DB) Set(ctx context.Context, model interface{}) *DB {
	// border case: input a nil as model, not allowed
	if model == nil {
		panic("can't input nil as a model")
	}
	_, ok := model.(Table)
	if !ok {
		panic("please set namespace and table name for this model")
	}
	return h
}
