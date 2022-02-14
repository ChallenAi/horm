package client

import (
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/challenai/horm/thrift/hbase"
)

// http Header attached to HBase client
// for example, some cloud service provider HBase instances need some authrization headers.
type Header struct {
	Key, Value string
}

// RoundTrip implemnt http RoundTripper interface
type RoundTripper struct {
	Headers []Header
}

// RoundTrip implemnt http RoundTripper interface
func (rt *RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	for _, header := range rt.Headers {
		req.Header.Add(header.Key, header.Value)
	}
	return http.DefaultTransport.RoundTrip(req)
}

// create a new hbase client
func NewHBaseClient(addr string, headers []Header) (*hbase.THBaseServiceClient, error) {
	httpClient := http.Client{
		Transport: &RoundTripper{
			Headers: headers,
		},
		Timeout: time.Second * 10,
	}
	trans, err := thrift.NewTHttpClientWithOptions(addr, thrift.THttpClientOptions{Client: &httpClient})
	if err != nil {
		return nil, err
	}
	err = trans.Open()
	if err != nil {
		return nil, err
	}
	proto := thrift.NewTBinaryProtocol(trans, false, false)
	thriftClient := thrift.NewTStandardClient(proto, proto)
	return hbase.NewTHBaseServiceClient(thriftClient), nil
}
