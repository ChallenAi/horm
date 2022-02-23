package horm

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/challenai/horm/codec"
	"github.com/challenai/horm/thrift/hbase"
)

const (
	HBaseTagHint    string = "horm"
	ModelName       string = "Model"
	BatchResultSize int32  = 1 << 6 // todo: selft-customized batchResultSize, default set to be 64KB (assume 1KB bytes per row)
)

// DB represent a HBase database
type DB struct {
	Error        error
	RowsAffected int64
	db           *hbase.THBaseServiceClient
	schemas      map[string]schema
	cdc          codec.Codec
}

// schema used to store struct field and column mapping information
type schema struct {
	col2field map[string]int
	field2col []string
}

// filter input raw filter string and rows limit to thrift server
type Filter struct {
	FilterString string
	Limit        int32
}

// create a new hbase database from thrift client
func NewDB(client *hbase.THBaseServiceClient, c codec.Codec) *DB {
	hb := &DB{
		db:      client,
		schemas: map[string]schema{},
		cdc:     c,
	}
	return hb
}

// HBase rows range query
func (h *DB) Find(ctx context.Context, list interface{}, startRow, stopRow string, selects []Column, filter *Filter) *DB {
	// border case: input a nil as model, not allowed
	if list == nil {
		panic("can't input nil as a model")
	}
	if reflect.TypeOf(list).Kind() != reflect.Ptr {
		panic("list should be a slice of struct pointer, for example: *[]User")
	}
	if reflect.TypeOf(list).Elem().Kind() != reflect.Slice {
		panic("list should be a slice of struct pointer, for example: *[]User")
	}
	modelType := reflect.TypeOf(list).Elem().Elem()
	if modelType.Kind() != reflect.Struct {
		panic("list should be a slice of struct pointer, for example: *[]User")
	}
	model := reflect.New(modelType).Interface()
	tb, ok := model.(Table)
	if !ok {
		panic("please set namespace and table name for this model")
	}

	tScan := &hbase.TScan{
		StartRow: []byte(startRow),
		StopRow:  []byte(stopRow),
	}
	if selects != nil && len(selects) > 0 {
		tScan.Columns = make([]*hbase.TColumn, 0, len(selects))
		for _, v := range selects {
			col := &hbase.TColumn{
				Family:    []byte(v.Family),
				Qualifier: []byte(v.Name),
			}
			if v.Timestamp != 0 {
				ts := int64(v.Timestamp)
				col.Timestamp = &ts
			}
			tScan.Columns = append(tScan.Columns, col)
		}
	}
	if filter != nil {
		if filter.FilterString != "" {
			tScan.FilterString = []byte(filter.FilterString)
		}
	}

	var scanResults []*hbase.TResult_
	resultSz := BatchResultSize
	listValue := reflect.ValueOf(list).Elem()
	for {
		var lastResult *hbase.TResult_ = nil
		// get query size in this batch
		if filter != nil {
			resultSz = getQuerySize(BatchResultSize, filter.Limit-int32(len(scanResults)))
			if resultSz == 0 {
				break
			}
		}
		currentResults, err := h.db.GetScannerResults(ctx, []byte(fmt.Sprintf("%s:%s", tb.Namespace(), tb.TableName())), tScan, resultSz)
		if err != nil {
			h.Error = err
			return h
		}
		for _, tResult := range currentResults {
			lastResult = tResult
			scanResults = append(scanResults, tResult)
		}
		if lastResult == nil {
			break
		} else {
			nextStartRow := getClosestRowAfter(lastResult.Row)
			tScan.StartRow = nextStartRow
		}
	}
	for _, v := range scanResults {
		m := reflect.New(modelType)
		mPtr := m.Elem()
		h.retrieveValue(&mPtr, v)
		listValue.Set(reflect.Append(listValue, m.Elem()))
	}
	return h
}

func getQuerySize(batchSz, diff int32) int32 {
	if diff <= 0 {
		return 0
	}
	if batchSz < diff {
		return batchSz
	}
	return diff
}

func getClosestRowAfter(row []byte) []byte {
	var nextRow []byte
	var i int
	for i = 0; i < len(row); i++ {
		nextRow = append(nextRow, row[i])
	}
	nextRow = append(nextRow, 0x00)
	return nextRow
}

func (h *DB) retrieveValue(value *reflect.Value, result *hbase.TResult_) {
	schm, ok := h.schemas[value.Type().Name()]
	if !ok {
		schm = h.registerModel(*value)
	}
	base := &Model{
		Rowkey: string(result.Row),
	}
	value.FieldByName(ModelName).Set(reflect.ValueOf(base))
	for _, v := range result.ColumnValues {
		key := fmt.Sprintf("%s:%s", v.Family, v.Qualifier)
		if idx, ok := schm.col2field[key]; ok {
			field := value.Field(idx)
			switch field.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				n, err := h.cdc.DecodeInt(v.GetValue())
				if err != nil {
					fmt.Println("failed to parse int column")
					panic(err)
				}
				field.SetInt(n)
			case reflect.Float32, reflect.Float64:
				n, err := h.cdc.DecodeFloat(v.GetValue())
				if err != nil {
					fmt.Println("failed to parse float column")
					panic(err)
				}
				field.SetFloat(n)
			case reflect.String:
				s, err := h.cdc.DecodeString(v.GetValue())
				if err != nil {
					fmt.Println("failed to parse string column")
					panic(err)
				}
				field.SetString(s)
			case reflect.Bool:
				b, err := h.cdc.DecodeBool(v.GetValue())
				if err != nil {
					fmt.Println("failed to parse bool column")
					panic(err)
				}
				field.SetBool(b)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				n, err := h.cdc.DecodeUint(v.GetValue())
				if err != nil {
					fmt.Println("failed to parse uint column")
					panic(err)
				}
				field.SetUint(n)
			}
		}
	}
}

// parse imported model so that we don't need to parse all the model fields everytime.
func (h *DB) registerModel(values reflect.Value) schema {
	schm := schema{}
	schm.field2col = make([]string, values.NumField())
	schm.col2field = map[string]int{}
	for i := 0; i < values.Type().NumField(); i++ {
		if values.Type().Field(i).Name == "Model" {
			continue
		}
		tagsList := strings.Split(values.Type().Field(i).Tag.Get(HBaseTagHint), ",")
		if len(tagsList) < 2 {
			panic("hbase column doesn't have column family or qualifier")
		}
		name := strings.Join(tagsList[:2], ":")
		schm.field2col[i] = name
		schm.col2field[name] = i
	}
	h.schemas[values.Type().Name()] = schm
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

	result, err := h.db.Get(ctx, []byte(fmt.Sprintf("%s:%s", tb.Namespace(), tb.TableName())), &hbase.TGet{Row: []byte(rowkey)})
	if err != nil {
		h.Error = err
		return h
	}

	value := reflect.ValueOf(model).Elem()
	h.retrieveValue(&value, result)

	return h
}

// insert or update model to HBase
func (h *DB) Set(ctx context.Context, model interface{}, selects []Column, rowkey string) *DB {
	// border case: input a nil as model, not allowed
	if model == nil {
		panic("can't input nil as a model")
	}
	_, ok := model.(Table)
	if !ok {
		panic("please set namespace and table name for this model")
	}
	schm, ok := h.schemas[reflect.TypeOf(model).Name()]
	vals := reflect.ValueOf(model)
	var columns []*hbase.TColumnValue

	if selects != nil && len(selects) > 0 {
		columns = make([]*hbase.TColumnValue, 0, len(selects))
		for _, v := range selects {
			col := &hbase.TColumnValue{
				Family:    []byte(v.Family),
				Qualifier: []byte(v.Name),
			}
			field := vals.Field(schm.col2field[fmt.Sprintf("%s:%s", v.Family, v.Name)])
			switch field.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				col.Value = h.cdc.EncodeInt(field.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				col.Value = h.cdc.EncodeUint(field.Uint())
			case reflect.Float32, reflect.Float64:
				col.Value = h.cdc.EncodeFloat(field.Float())
			case reflect.String:
				col.Value = h.cdc.EncodeString(field.String())
			case reflect.Bool:
				col.Value = h.cdc.EncodeBool(field.Bool())
			}
			if v.Timestamp != 0 {
				ts := int64(v.Timestamp)
				col.Timestamp = &ts
			}
			columns = append(columns, col)
		}
	}
	return h
}

func ValidateType() {

}

func (h *DB) BatchSet(ctx context.Context, model interface{}) *DB {
	return h
}

func (h *DB) Delete(ctx context.Context, model interface{}, rowkey string) *DB {
	return h
}

func (h *DB) DeleteAll(ctx context.Context, model interface{}) *DB {
	return h
}
