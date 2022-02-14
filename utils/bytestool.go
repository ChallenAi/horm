package utils

import (
	"bytes"
	"encoding/binary"
	"strconv"
)

func EncodeInt(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

func DecodeInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

func EncodeIntStr(n int) []byte {
	return []byte(strconv.Itoa(n))
}

func DecodeIntStr(b []byte) (int, error) {
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, err
	}
	return n, nil
}
