package horm

import (
	"github.com/challenai/horm/client"
	c "github.com/challenai/horm/codec"
	"github.com/challenai/horm/logger"
)

// NewHBase create a new HBase DB
func NewHBase(addr string, headers []client.Header) (*DB, error) {
	client, err := client.NewHBaseClient(addr, headers)
	if err != nil {
		return nil, err
	}
	hb := NewDB(client, &Conf{
		cdc: &c.DefaultCodec{},
		log: logger.NewStdLogger(),
	})
	return hb, nil
}

// NewHBase create a new HBase DB
func NewHBaseWithCodec(addr string, headers []client.Header, codec c.Codec) (*DB, error) {
	client, err := client.NewHBaseClient(addr, headers)
	if err != nil {
		return nil, err
	}
	hb := NewDB(client, &Conf{
		cdc: codec,
		log: logger.NewStdLogger(),
	})
	return hb, nil
}
