package decoder

import (
	"bytes"
	"encoding/binary"
)

type BytesParser struct {
	data   []byte
	limit  int
	offset int
}

func NewBytesParser(data []byte) *BytesParser {
	return &BytesParser{data: data, limit: len(data), offset: 0}
}

func (p *BytesParser) ReadInt8() int8 {
	n := int8(p.data[p.offset])
	p.offset++
	return n
}

func (p *BytesParser) ReadInt16() int16 {
	var n int16
	binary.Read(bytes.NewReader(p.data[p.offset:p.offset+2]), binary.BigEndian, &n)
	p.offset += 2
	return n
}

func (p *BytesParser) ReadInt32() int32 {
	var n int32
	binary.Read(bytes.NewReader(p.data[p.offset:p.offset+4]), binary.BigEndian, &n)
	p.offset += 4
	return n
}

func (p *BytesParser) ReadCompactString() string {
	len := int(p.ReadInt8() - 1)
	if len <= 0 {
		return ""
	}
	value := string(p.data[p.offset : p.offset+len])
	p.offset += len
	return value
}

func (p *BytesParser) ReadNullableString() string {
	length := int(p.ReadInt16())
	if length == -1 {
		return ""
	}
	value := string(p.data[p.offset : p.offset+length])
	p.offset += length
	return value
}
