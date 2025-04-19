package utils

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"io"
	"log"
	"net"
	"os"
)

func WriteFile(file *os.File, result string) {
	message := []byte(result)

	writtenBytes := 0
	totalBytes := len(message)

	for writtenBytes < totalBytes {
		n, err := file.Write(message[writtenBytes:])
		if err != nil {
			return
		}

		writtenBytes += n
	}
}

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

func ReadMessage(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)

	lengthBytes := make([]byte, 2)
	_, err := io.ReadFull(reader, lengthBytes)
	if err != nil {
		return "", err
	}

	bytesRead := 0
	messageLength := binary.BigEndian.Uint16(lengthBytes)

	messageBytes := make([]byte, messageLength)

	for bytesRead < int(messageLength) {
		n, err := reader.Read(messageBytes[bytesRead:])
		if err != nil {
			return "", err
		}

		bytesRead += n
	}

	return string(messageBytes), nil
}

func IgnoreFirstCSVLine(reader *csv.Reader) {
	_, err := reader.Read()
	if err != nil {
		log.Fatal(err)
	}
}

func addMessageLengthPrefix(message []byte) []byte {
	msgLength := uint16(len(message))
	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, msgLength)

	return append(lengthBytes, message...)
}
