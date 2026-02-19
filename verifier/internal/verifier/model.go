package verifier

import (
	"fmt"
	"strings"

	"github.com/anishathalye/porcupine"
)

// createKVModel creates a Porcupine model for a key-value store
func createKVModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return make(map[string]string)
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			st := state.(map[string]string)
			in := input.(map[string]interface{})
			out := output.(map[string]interface{})

			opType, ok := in["type"].(string)
			if !ok {
				return false, st
			}

			switch opType {
			case "Put":
				return handlePut(st, in)
			case "Get":
				return handleGet(st, in, out)
			case "Delete":
				return handleDelete(st, in)
			default:
				return false, st
			}
		},
		DescribeOperation: describeOperation,
		DescribeState:     describeState,
		Partition:         partitionByKey,
	}
}

func handlePut(st map[string]string, in map[string]interface{}) (bool, map[string]string) {
	key, ok1 := in["key"].(string)
	value, ok2 := in["value"].(string)
	if !ok1 || !ok2 {
		return false, st
	}
	st[key] = value
	return true, st
}

func handleGet(st map[string]string, in map[string]interface{}, out map[string]interface{}) (bool, map[string]string) {
	key, ok1 := in["key"].(string)
	if !ok1 {
		return false, st
	}
	expectedValue, exists := st[key]
	outStatus, ok2 := out["status"].(string)
	if !ok2 || outStatus != "ok" {
		return false, st
	}
	outValue, _ := out["value"]
	if !exists {
		// Key doesn't exist, output should be nil
		return outValue == nil, st
	}
	// Key exists, check if output matches
	if outValue == nil {
		return false, st
	}
	outValueStr, ok4 := outValue.(string)
	return ok4 && outValueStr == expectedValue, st
}

func handleDelete(st map[string]string, in map[string]interface{}) (bool, map[string]string) {
	key, ok1 := in["key"].(string)
	if !ok1 {
		return false, st
	}
	delete(st, key)
	return true, st
}

func describeOperation(input, output interface{}) string {
	in := input.(map[string]interface{})
	out := output.(map[string]interface{})

	opType, _ := in["type"].(string)
	key, _ := in["key"].(string)

	switch opType {
	case "Put":
		value, _ := in["value"].(string)
		return fmt.Sprintf("Put('%s', '%s')", key, value)
	case "Get":
		outValue, exists := out["value"]
		if !exists || outValue == nil {
			return fmt.Sprintf("Get('%s') -> nil", key)
		}
		return fmt.Sprintf("Get('%s') -> '%s'", key, outValue)
	case "Delete":
		return fmt.Sprintf("Delete('%s')", key)
	default:
		return fmt.Sprintf("%v -> %v", input, output)
	}
}

func describeState(state interface{}) string {
	st := state.(map[string]string)
	if len(st) == 0 {
		return "{}"
	}
	var parts []string
	for k, v := range st {
		parts = append(parts, fmt.Sprintf("'%s': '%s'", k, v))
	}
	return fmt.Sprintf("{%s}", strings.Join(parts, ", "))
}

func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	partitions := make(map[string][]porcupine.Operation)
	for _, op := range history {
		in := op.Input.(map[string]interface{})
		key, ok := in["key"].(string)
		if !ok {
			key = "__unknown__"
		}
		partitions[key] = append(partitions[key], op)
	}
	result := make([][]porcupine.Operation, 0, len(partitions))
	for _, ops := range partitions {
		result = append(result, ops)
	}
	return result
}
