package horm

import "github.com/challenai/horm/client"

// NewHBase create a new HBase DB
func NewHBase(addr string, headers []client.Header) (*DB, error) {
	client, err := client.NewHBaseClient(addr, headers)
	if err != nil {
		return nil, err
	}
	hb := NewDB(client)
	return hb, nil
}
