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
		Cdc: &c.DefaultCodec{},
		Log: logger.NewStdLogger(),
	})
	return hb, nil
}

// NewHBaseWithLog create a new HBase DB
func NewHBaseWithLog(addr string, headers []client.Header, log logger.Logger) (*DB, error) {
	client, err := client.NewHBaseClient(addr, headers)
	if err != nil {
		return nil, err
	}
	hb := NewDB(client, &Conf{
		Cdc: &c.DefaultCodec{},
		Log: log,
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
		Cdc: codec,
		Log: logger.NewStdLogger(),
	})
	return hb, nil
}
