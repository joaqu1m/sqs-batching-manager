package entities

import (
	"encoding/json"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type MessageAttributes map[string]types.MessageAttributeValue

func NewMessageAttributesFromMap(input map[string]any) MessageAttributes {

	result := make(MessageAttributes, len(input))
	for key, value := range input {

		reflectedValue := reflect.ValueOf(value)

		switch reflectedValue.Kind() {
		case reflect.String:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(reflectedValue.String()),
			}
		case reflect.Bool:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(strconv.FormatBool(reflectedValue.Bool())),
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.FormatInt(reflectedValue.Int(), 10)),
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.FormatUint(reflectedValue.Uint(), 10)),
			}
		case reflect.Float32, reflect.Float64:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.FormatFloat(reflectedValue.Float(), 'f', -1, 64)),
			}
		}

	}

	return result
}

func (ma MessageAttributes) ToMap() map[string]any {
	result := make(map[string]any, len(ma))
	for key, value := range ma {
		switch *value.DataType {
		case "String":
			result[key] = *value.StringValue
		case "Number":
			result[key] = *value.StringValue
		case "Binary":
			result[key] = value.BinaryValue
		}
	}

	return result
}

func NewMessageAttributesFromBytes(input []byte) (MessageAttributes, error) {
	var rawMap map[string]any
	if err := json.Unmarshal(input, &rawMap); err != nil {
		return nil, err
	}

	return NewMessageAttributesFromMap(rawMap), nil
}

func (ma MessageAttributes) Marshal() ([]byte, error) {
	return json.Marshal(ma.ToMap())
}

func (ma MessageAttributes) IsEmpty() bool {
	return len(ma) == 0
}

func (ma MessageAttributes) GetAWSSizeInBytes() uint64 {
	totalSize := 0

	// AWS Size Calculation:

	for name, attr := range ma {
		// 1. Attribute Name
		totalSize += len(name)

		// 2. Data Type
		if attr.DataType != nil {
			totalSize += len(*attr.DataType)
		}

		// 3. Value
		if attr.StringValue != nil {
			totalSize += len(*attr.StringValue)
		} else if len(attr.BinaryValue) > 0 {
			totalSize += len(attr.BinaryValue)
		}
	}

	return uint64(totalSize)
}
