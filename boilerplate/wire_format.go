package boilerplate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	magicByte byte = 0x0

	wireSize = 5
	idSize   = 4
)

func wireMessage(id uint32, payload []byte) ([]byte, error) {
	var buf bytes.Buffer

	err := buf.WriteByte(magicByte)
	if err != nil {
		return nil, err
	}

	idBytes := make([]byte, idSize)
	binary.BigEndian.PutUint32(idBytes, id)

	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(payload)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func extractSchemaID(data []byte) (uint32, error) {
	err := checkWireFormat(data)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(data[1:5]), nil
}

func extractPayload(data []byte) ([]byte, error) {
	err := checkWireFormat(data)
	if err != nil {
		return nil, err
	}

	return data[wireSize:], nil
}

func checkWireFormat(data []byte) error {
	if len(data) < wireSize {
		return errors.New("data too short")
	}

	if data[0] != magicByte {
		return fmt.Errorf("invalid magic byte: %x", data[0])
	}

	return nil
}
