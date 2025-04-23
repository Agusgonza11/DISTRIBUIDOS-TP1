package utils

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
)

func WriteMessage(conn net.Conn, message []byte) error {
	message = addMessageLengthPrefix(message)

	writer := bufio.NewWriter(conn)

	writtenBytes := 0
	totalBytes := len(message)

	for writtenBytes < totalBytes {
		n, err := writer.Write(message[writtenBytes:])
		if err != nil {
			return err
		}

		writtenBytes += n
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	return nil
}

func ReadMessage(reader *bufio.Reader) (string, error) {
	lengthBytes := make([]byte, 2)

	if err := ReadFull(reader, lengthBytes, len(lengthBytes)); err != nil {
		return "", err
	}

	messageLength := binary.BigEndian.Uint16(lengthBytes)
	messageBytes := make([]byte, messageLength)

	if err := ReadFull(reader, messageBytes, int(messageLength)); err != nil {
		return "", err
	}

	return string(messageBytes), nil
}

func ReadFull(reader *bufio.Reader, messageBytes []byte, n int) error {
	totalBytesRead := 0

	for totalBytesRead < n {
		currentBytesRead, err := reader.Read(messageBytes[totalBytesRead:])
		if err != nil {
			return err
		}

		if currentBytesRead == 0 {
			return io.EOF
		}

		totalBytesRead += currentBytesRead
	}

	return nil
}

func addMessageLengthPrefix(message []byte) []byte {
	msgLength := uint16(len(message))
	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, msgLength)

	return append(lengthBytes, message...)
}
