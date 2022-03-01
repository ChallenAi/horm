package horm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/challenai/horm/codec"
	"github.com/challenai/horm/logger"
	"github.com/challenai/horm/thrift/hbase"
)

const (
	HBaseTagHint    string = "horm"
	ModelName       string = "Model"
	RowName         string = "Rowkey"
	BatchResultSize int32  = 1 << 6 // todo: selft-customized batchResultSize, default set to be 64KB (assume 1KB bytes per row)
)

// DB represent a HBase database
type DB struct {
	Error        error
	RowsAffected int64
	db           *hbase.THBaseServiceClient
	schemas      map[string]schema
	cdc          codec.Codec
	log          logger.Logger
}

type Conf struct {
	cdc codec.Codec
	log logger.Logger
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
func NewDB(client *hbase.THBaseServiceClient, conf *Conf) *DB {
	hb := &DB{
		db:      client,
		schemas: map[string]schema{},
		cdc:     conf.cdc,
		log:     conf.log,
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
		tagsStr := values.Type().Field(i).Tag.Get(HBaseTagHint)
		if tagsStr == "" || tagsStr == "-" {
			continue
		}
		tagsList := strings.Split(tagsStr, ",")
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
	h.log.Infof("start to set row")
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
func (h *DB) Set(ctx context.Context, model interface{}, selects []Column) *DB {
	h.log.Infof("start to set row")
	// border case: input a nil as model, not allowed
	if model == nil {
		panic("can't input nil as a model")
	}
	tb, ok := model.(Table)
	if !ok {
		panic("please set namespace and table name for this model")
	}

	value := reflect.ValueOf(model).Elem()
	put := &hbase.TPut{}
	h.injectValue(&value, put, selects)
	err := h.db.Put(ctx, []byte(fmt.Sprintf("%s:%s", tb.Namespace(), tb.TableName())), put)
	h.Error = err
	return h
}

func (h *DB) injectValue(value *reflect.Value, put *hbase.TPut, selects []Column) {
	if put == nil {
		return
	}
	schm, ok := h.schemas[value.Type().Name()]
	if !ok {
		schm = h.registerModel(*value)
	}

	// todo: assert horm.Model is a pointer when verify basic model extend
	// fmt.Println(value.FieldByName(ModelName).Elem().FieldByName(RowName).String()))
	put.Row = []byte(value.FieldByName(ModelName).Elem().FieldByName(RowName).String())
	put.ColumnValues = []*hbase.TColumnValue{}

	if selects != nil && len(selects) > 0 {
		for _, v := range selects {
			field := value.Field(schm.col2field[fmt.Sprintf("%s:%s", v.Family, v.Name)])
			col := &hbase.TColumnValue{}
			h.buildColumn(v.Family, v.Name, &field, col)
			put.ColumnValues = append(put.ColumnValues, col)
		}
	} else {
		for i, v := range schm.field2col {
			if v != "" {
				descriptors := strings.Split(v, ":")
				if len(descriptors) == 2 {
					field := value.Field(i)
					col := &hbase.TColumnValue{}
					h.buildColumn(descriptors[0], descriptors[1], &field, col)
					put.ColumnValues = append(put.ColumnValues, col)
				}
			}
		}
	}
}

func (h *DB) buildColumn(family, qualifier string, field *reflect.Value, columnValue *hbase.TColumnValue) {
	columnValue.Family = []byte(family)
	columnValue.Qualifier = []byte(qualifier)
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		columnValue.Value = h.cdc.EncodeInt(field.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		columnValue.Value = h.cdc.EncodeUint(field.Uint())
	case reflect.Float32, reflect.Float64:
		columnValue.Value = h.cdc.EncodeFloat(field.Float())
	case reflect.String:
		columnValue.Value = h.cdc.EncodeString(field.String())
	case reflect.Bool:
		columnValue.Value = h.cdc.EncodeBool(field.Bool())
	}
}

func validateListable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice:
		fallthrough
	case reflect.Array:
		return true
	}
	err := fmt.Errorf("invalid row type to report, should be a slice but got %s", t)
	panic(err)
	// return false
}

func (h *DB) BatchSet(ctx context.Context, rows interface{}, selects []Column) *DB {
	h.log.Infof("start to batch set rows")
	if !validateListable(reflect.TypeOf(rows)) {
		h.Error = errors.New("batchSet need a slice as input, like []User")
		return h
	}
	v := reflect.ValueOf(rows)
	puts := []*hbase.TPut{}
	for i := 0; i < v.Len(); i++ {
		field := v.Index(i)
		fmt.Println(field)
		put := &hbase.TPut{}
		h.injectValue(&field, put, selects)
		puts = append(puts, put)
	}
	if v.Len() > 0 {
		modelType := v.Index(0).Type()
		m := reflect.New(modelType)
		tb, ok := m.Interface().(Table)
		if !ok {
			panic("please set namespace and table name for this model")
		}
		err := h.db.PutMultiple(ctx, []byte(fmt.Sprintf("%s:%s", tb.Namespace(), tb.TableName())), puts)
		h.Error = err
	}
	return h
}

func (h *DB) Delete(ctx context.Context, model interface{}, rowkey string) *DB {
	return h
}

func (h *DB) DeleteAll(ctx context.Context, model interface{}) *DB {
	return h
}
