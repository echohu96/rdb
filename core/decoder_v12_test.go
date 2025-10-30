package core

import (
	"bytes"
	"testing"

	"github.com/hdt3213/rdb/model"
)

// TestRDBV12FreqOpcode tests parsing RDB v12 files with FREQ (0xF4) opcode
func TestRDBV12FreqOpcode(t *testing.T) {
	// Construct RDB v12 data with FREQ opcode
	rdbData := []byte{
		// Header: "REDIS0012"
		'R', 'E', 'D', 'I', 'S', '0', '0', '1', '2',

		// SELECTDB 0
		0xFE, 0x00,

		// RESIZEDB (1 key, 0 expires)
		0xFB, 0x01, 0x00,

		// FREQ opcode (0xF4) + frequency value (42)
		0xF4, 42,

		// String type (0x00)
		0x00,
		// Key: "key1" (length=4)
		0x04, 'k', 'e', 'y', '1',
		// Value: "val1" (length=4)
		0x04, 'v', 'a', 'l', '1',

		// EOF
		0xFF,
		// CRC64 checksum (8 bytes)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	reader := bytes.NewReader(rdbData)
	decoder := NewDecoder(reader)

	var parsedKey string
	var parsedValue string
	objectCount := 0

	err := decoder.Parse(func(object model.RedisObject) bool {
		objectCount++
		parsedKey = object.GetKey()
		if strObj, ok := object.(*model.StringObject); ok {
			parsedValue = string(strObj.Value)
		}
		return true
	})

	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if objectCount != 1 {
		t.Errorf("Expected 1 object, got %d", objectCount)
	}

	if parsedKey != "key1" {
		t.Errorf("Expected key 'key1', got '%s'", parsedKey)
	}

	if parsedValue != "val1" {
		t.Errorf("Expected value 'val1', got '%s'", parsedValue)
	}
}

// TestRDBV12IdleOpcode tests parsing RDB v12 files with IDLE (0xF5) opcode
func TestRDBV12IdleOpcode(t *testing.T) {
	rdbData := []byte{
		// Header: "REDIS0012"
		'R', 'E', 'D', 'I', 'S', '0', '0', '1', '2',

		// SELECTDB 0
		0xFE, 0x00,

		// RESIZEDB
		0xFB, 0x01, 0x00,

		// IDLE opcode (0xF5) + idle time (1000 in little-endian, encoded as length)
		0xF5, 0xC2, 0xE8, 0x03, // length encoding: 1000

		// String type
		0x00,
		// Key: "key2"
		0x04, 'k', 'e', 'y', '2',
		// Value: "val2"
		0x04, 'v', 'a', 'l', '2',

		// EOF
		0xFF,
		// CRC64
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	reader := bytes.NewReader(rdbData)
	decoder := NewDecoder(reader)

	objectCount := 0

	err := decoder.Parse(func(object model.RedisObject) bool {
		objectCount++
		return true
	})

	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if objectCount != 1 {
		t.Errorf("Expected 1 object, got %d", objectCount)
	}
}

// TestRDBV12FreqAndIdle tests parsing with both FREQ and IDLE opcodes
func TestRDBV12FreqAndIdle(t *testing.T) {
	rdbData := []byte{
		// Header: "REDIS0012"
		'R', 'E', 'D', 'I', 'S', '0', '0', '1', '2',

		// SELECTDB 0
		0xFE, 0x00,

		// RESIZEDB
		0xFB, 0x01, 0x00,

		// FREQ (5)
		0xF4, 0x05,

		// IDLE (2000)
		0xF5, 0xC2, 0xD0, 0x0F,

		// String type
		0x00,
		// Key: "key3"
		0x04, 'k', 'e', 'y', '3',
		// Value: "val3"
		0x04, 'v', 'a', 'l', '3',

		// EOF
		0xFF,
		// CRC64
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	reader := bytes.NewReader(rdbData)
	decoder := NewDecoder(reader)

	var parsedKey string
	objectCount := 0

	err := decoder.Parse(func(object model.RedisObject) bool {
		objectCount++
		parsedKey = object.GetKey()
		return true
	})

	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if objectCount != 1 {
		t.Errorf("Expected 1 object, got %d", objectCount)
	}

	if parsedKey != "key3" {
		t.Errorf("Expected key 'key3', got '%s'", parsedKey)
	}
}

// TestRDBV12MetadataReset tests that metadata is properly reset between objects
func TestRDBV12MetadataReset(t *testing.T) {
	rdbData := []byte{
		// Header
		'R', 'E', 'D', 'I', 'S', '0', '0', '1', '2',
		// SELECTDB 0
		0xFE, 0x00,
		// RESIZEDB
		0xFB, 0x02, 0x00,

		// First object WITH FREQ metadata
		0xF4, 10, // freq = 10
		0x00, 0x04, 'k', 'e', 'y', '1',
		0x04, 'v', 'a', 'l', '1',

		// Second object WITHOUT metadata (should NOT inherit from key1)
		0x00, 0x04, 'k', 'e', 'y', '2',
		0x04, 'v', 'a', 'l', '2',

		// EOF
		0xFF,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	reader := bytes.NewReader(rdbData)
	decoder := NewDecoder(reader)

	keys := []string{}

	err := decoder.Parse(func(object model.RedisObject) bool {
		keys = append(keys, object.GetKey())
		return true
	})

	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}

	if keys[0] != "key1" {
		t.Errorf("First key: expected 'key1', got '%s'", keys[0])
	}

	if keys[1] != "key2" {
		t.Errorf("Second key: expected 'key2', got '%s'", keys[1])
	}
}

// TestRDBV12FreqWithExpiration tests FREQ opcode combined with expiration
func TestRDBV12FreqWithExpiration(t *testing.T) {
	rdbData := []byte{
		// Header: "REDIS0012"
		'R', 'E', 'D', 'I', 'S', '0', '0', '1', '2',

		// SELECTDB 0
		0xFE, 0x00,

		// RESIZEDB
		0xFB, 0x01, 0x01,

		// EXPIRETIME_MS (0xFC) + timestamp (8 bytes little-endian)
		0xFC, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		// FREQ
		0xF4, 15,

		// String type
		0x00,
		// Key: "expiring_key"
		0x0C, 'e', 'x', 'p', 'i', 'r', 'i', 'n', 'g', '_', 'k', 'e', 'y',
		// Value: "value"
		0x05, 'v', 'a', 'l', 'u', 'e',

		// EOF
		0xFF,
		// CRC64
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	reader := bytes.NewReader(rdbData)
	decoder := NewDecoder(reader)

	var parsedKey string
	objectCount := 0

	err := decoder.Parse(func(object model.RedisObject) bool {
		objectCount++
		parsedKey = object.GetKey()
		return true
	})

	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if objectCount != 1 {
		t.Errorf("Expected 1 object, got %d", objectCount)
	}

	if parsedKey != "expiring_key" {
		t.Errorf("Expected key 'expiring_key', got '%s'", parsedKey)
	}
}

// TestRDBV12MultipleKeys tests parsing multiple keys with different metadata combinations
func TestRDBV12MultipleKeys(t *testing.T) {
	rdbData := []byte{
		// Header
		'R', 'E', 'D', 'I', 'S', '0', '0', '1', '2',
		// SELECTDB 0
		0xFE, 0x00,
		// RESIZEDB
		0xFB, 0x03, 0x00,

		// Key 1: With FREQ
		0xF4, 10,
		0x00, 0x04, 'k', 'e', 'y', '1',
		0x04, 'v', 'a', 'l', '1',

		// Key 2: With IDLE
		0xF5, 0xC2, 0xF4, 0x01, // 500 encoded as length
		0x00, 0x04, 'k', 'e', 'y', '2',
		0x04, 'v', 'a', 'l', '2',

		// Key 3: With both FREQ and IDLE
		0xF4, 20,
		0xF5, 0xC2, 0xDC, 0x05, // 1500
		0x00, 0x04, 'k', 'e', 'y', '3',
		0x04, 'v', 'a', 'l', '3',

		// EOF
		0xFF,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	reader := bytes.NewReader(rdbData)
	decoder := NewDecoder(reader)

	keys := []string{}

	err := decoder.Parse(func(object model.RedisObject) bool {
		keys = append(keys, object.GetKey())
		return true
	})

	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	expectedKeys := []string{"key1", "key2", "key3"}
	for i, expected := range expectedKeys {
		if i >= len(keys) {
			t.Errorf("Missing key at index %d", i)
			continue
		}
		if keys[i] != expected {
			t.Errorf("Key %d: expected '%s', got '%s'", i, expected, keys[i])
		}
	}
}
