package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/netxops/frame/dataframe"
	"github.com/netxops/frame/series"
)

func MapToDataFrame(data interface{}, topColumn string, strictMode bool, paths ...string) (dataframe.DataFrame, error) {
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Map {
		return dataframe.New(), fmt.Errorf("input must be a map")
	}

	// Get all keys and sort them
	keys := v.MapKeys()
	sortedKeys := make([]string, len(keys))
	for i, key := range keys {
		sortedKeys[i] = key.String()
	}
	sort.Strings(sortedKeys)

	var resultDF dataframe.DataFrame
	for index, keyStr := range sortedKeys {
		key := reflect.ValueOf(keyStr)
		value := v.MapIndex(key)
		if value.Kind() == reflect.Interface {
			value = value.Elem()
		}

		df, err := FlexibleToDataFrame(value.Interface(), strictMode, paths...)
		if err != nil {
			return dataframe.New(), err
		}

		keySeries := make([]string, df.Nrow())
		for i := range df.Nrow() {
			keySeries[i] = keyStr
		}
		names := []string{topColumn}
		names = append(names, df.Names()...)

		df = df.Mutate(series.New(keySeries, series.String, topColumn))
		df = df.Select(names)
		if index == 0 {
			resultDF = df
		} else {
			resultDF = resultDF.Concat(df)
		}
	}
	return resultDF, resultDF.Error()
}

func FlexibleToDataFrame(data interface{}, strictMode bool, paths ...string) (dataframe.DataFrame, error) {
	var df dataframe.DataFrame
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		df := dataframe.New()
		df.Err = fmt.Errorf("input must be a slice")
		return df, df.Error()
	}

	// Create an empty DataFrame
	if v.Len() == 0 {
		// If the input slice is empty, add empty series for each path
		for _, path := range paths {
			s := series.New([]interface{}{}, series.String, path)
			df = df.Mutate(s)
		}
		return df, df.Error()
	}

	// Fill series with data
	for index, path := range paths {
		s, err := createSeriesFromPath(v, path, strictMode)
		if err != nil {
			return dataframe.New(), err
		}
		if index == 0 {
			df = dataframe.New(s)
		} else {
			df = df.Mutate(s)
		}

	}

	return df, df.Error()
}

func createSeriesFromPath(v reflect.Value, path string, strictMode bool) (series.Series, error) {
	data := make([]interface{}, v.Len())
	var err error
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		data[i], err = GetValueByPath(elem, path)
		if err != nil {
			if strictMode {
				s := series.Strings("")
				s.Err = fmt.Errorf("error extracting value from path %s for element %d: %v", path, i, err)
				return s, s.Error()
			}
			data[i] = nil
		}
	}
	return createSeriesFromData(data, path)
}

func createSeriesFromData(data []interface{}, name string) (series.Series, error) {
	if len(data) == 0 {
		return series.Series{}, fmt.Errorf("error creating series for path %s: data is empty", name)
	}

	var t series.Type
	newData := make([]interface{}, len(data))

	// Determine the type based on the first non-nil element
	for _, v := range data {
		if v != nil {
			switch reflect.TypeOf(v).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				t = series.Int
			case reflect.Float32, reflect.Float64:
				t = series.Float
			case reflect.Bool:
				t = series.Bool
			default:
				t = series.String
			}
			break
		}
	}

	for i, v := range data {
		if v == nil {
			newData[i] = nil
			continue
		}

		switch t {
		case series.Int:
			switch reflect.TypeOf(v).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				newData[i] = int(reflect.ValueOf(v).Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				newData[i] = int(reflect.ValueOf(v).Uint())
			default:
				// If the value is not an integer type, set it to nil
				newData[i] = nil
			}
		case series.Float:
			if floatVal, ok := v.(float64); ok {
				newData[i] = floatVal
			} else if floatVal, ok := v.(float32); ok {
				newData[i] = float64(floatVal)
			} else {
				// If the value is not a float64 or float32, set it to nil
				newData[i] = nil
			}
		case series.Bool:
			if boolVal, ok := v.(bool); ok {
				newData[i] = boolVal
			} else {
				// If the value is not a bool, set it to nil
				newData[i] = nil
			}
		default:
			// For string and other types
			switch reflect.TypeOf(v).Kind() {
			case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
				newData[i] = toJSON(v)
			default:
				newData[i] = fmt.Sprintf("%v", v)
			}
		}
	}

	return series.New(newData, t, name), nil
}

func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func GetValueByPath(data interface{}, path string) (interface{}, error) {
	if path == "" {
		return nil, fmt.Errorf("empty path is not allowed")
	}

	v := reflect.ValueOf(data)
	keys := strings.Split(path, ".")
	visited := make(map[uintptr]bool)

	for keyIndex, key := range keys {
		if !v.IsValid() {
			return nil, fmt.Errorf("invalid value encountered at key: %s", key)
		}

		// Dereference pointer if v is a pointer
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return nil, fmt.Errorf("nil pointer encountered at key: %s", key)
			}
			ptr := v.Pointer()
			if visited[ptr] {
				return nil, fmt.Errorf("circular reference detected at key: %s", key)
			}
			visited[ptr] = true
			v = v.Elem()
		}

		switch v.Kind() {
		case reflect.Struct:
			field := v.FieldByName(key)
			if !field.IsValid() {
				return nil, fmt.Errorf("field not found: %s", key)
			}
			if field.Kind() == reflect.Func {
				return nil, fmt.Errorf("unsupported type: %s at key: %s", field.Kind(), key)
			}
			v = field
		case reflect.Map:
			if v.IsNil() {
				return nil, fmt.Errorf("nil map encountered at key: %s", key)
			}
			v = v.MapIndex(reflect.ValueOf(key))
			if !v.IsValid() {
				return nil, fmt.Errorf("key not found in map: %s", key)
			}
			if keyIndex < len(keys)-1 {
				switch v.Kind() {
				case reflect.Interface:
					if v.IsNil() {
						return nil, fmt.Errorf("nil interface encountered at key: %s", key)
					}
					v = v.Elem()
					if !v.IsValid() {
						return nil, fmt.Errorf("invalid value after dereferencing interface at key: %s", key)
					}
				}
			}
		case reflect.Slice, reflect.Array:
			index, err := strconv.Atoi(key)
			if err != nil {
				return nil, fmt.Errorf("invalid array index at key: %s", key)
			}
			if index < 0 || index >= v.Len() {
				return nil, fmt.Errorf("array index out of bounds at key: %s", key)
			}
			v = v.Index(index)
			if !v.IsValid() {
				return nil, fmt.Errorf("key not found in map: %s", key)
			}

			if keyIndex < len(keys)-1 {
				switch v.Kind() {
				case reflect.Interface:
					if v.IsNil() {
						return nil, fmt.Errorf("nil interface encountered at key: %s", key)
					}
					v = v.Elem()
					if !v.IsValid() {
						return nil, fmt.Errorf("invalid value after dereferencing interface at key: %s", key)
					}
				}
			}

		case reflect.Interface:
			if v.IsNil() {
				return nil, fmt.Errorf("nil interface encountered at key: %s", key)
			}
			v = v.Elem()
			if !v.IsValid() {
				return nil, fmt.Errorf("invalid value after dereferencing interface at key: %s", key)
			}
			// After dereferencing the interface, we need to reprocess this key
			continue
		case reflect.Func:
			return nil, fmt.Errorf("unsupported type: %s at key: %s", v.Kind(), key)
		default:
			return nil, fmt.Errorf("unsupported type: %v at key: %s", v.Kind(), key)
		}
	}

	if !v.IsValid() {
		return nil, fmt.Errorf("invalid value at end of path")
	}

	return v.Interface(), nil
}
