package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type Codec interface {
	EncodeInt(int64) []byte
	DecodeInt([]byte) (int64, error)
	EncodeFloat(float64) []byte
	DecodeFloat([]byte) (float64, error)
	EncodeBool(bool) []byte
	DecodeBool([]byte) (bool, error)
	EncodeString(string) []byte
	DecodeString([]byte) (string, error)
	EncodeUint(uint64) []byte
	DecodeUint([]byte) (uint64, error)
}

type DefaultCodec struct{}

func (*DefaultCodec) EncodeInt(n int64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func (*DefaultCodec) DecodeInt(b []byte) (int64, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var x int64
	err := binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x, err
}

func (*DefaultCodec) EncodeFloat(n float64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func (*DefaultCodec) DecodeFloat(b []byte) (float64, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var x float64
	err := binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x, err
}

func (*DefaultCodec) EncodeBool(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func (*DefaultCodec) DecodeBool(b []byte) (bool, error) {
	if len(b) != 1 {
		return false, errors.New("failed to parse bytes to bool, invalid bool encoding")
	}
	return bytes.Equal(b, []byte{1}), nil
}

func (*DefaultCodec) EncodeString(s string) []byte {
	return []byte(s)
}

func (*DefaultCodec) DecodeString(b []byte) (string, error) {
	return string(b), nil
}

func (*DefaultCodec) EncodeUint(n uint64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func (*DefaultCodec) DecodeUint(b []byte) (uint64, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var n uint64
	err := binary.Read(bytesBuffer, binary.BigEndian, &n)
	return n, err
}
