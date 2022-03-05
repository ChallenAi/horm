package horm

// Model should implement Table interface to specify the namespace and table name.
type Table interface {
	Namespace() string
	TableName() string
}

// base model for every hbase model
type Model struct {
	Rowkey string
}

// column is a column in HBase column family, used to pick columns to query
type Column struct {
	Family    string
	Name      string
	Timestamp int64
}

type Row struct {
	Rowkey  string
	Columns []Column
}
