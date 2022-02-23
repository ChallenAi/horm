package horm

import (
	"github.com/challenai/horm/client"
	c "github.com/challenai/horm/codec"
)

// NewHBase create a new HBase DB
func NewHBase(addr string, headers []client.Header) (*DB, error) {
	client, err := client.NewHBaseClient(addr, headers)
	if err != nil {
		return nil, err
	}
	hb := NewDB(client, &c.DefaultCodec{})
	return hb, nil
}

// NewHBase create a new HBase DB
func NewHBaseCodec(addr string, headers []client.Header, codec c.Codec) (*DB, error) {
	client, err := client.NewHBaseClient(addr, headers)
	if err != nil {
		return nil, err
	}
	hb := NewDB(client, codec)
	return hb, nil
}
