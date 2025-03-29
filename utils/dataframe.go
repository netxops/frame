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

// data := map[string]interface{}{
//     "A": []map[string]interface{}{
//         {"x": 1, "y": 2},
//         {"x": 3, "y": 4},
//     },
//     "B": []map[string]interface{}{
//         {"x": 5, "y": 6},
//         {"x": 7, "y": 8},
//     },
// }
// topColumn := "key"
// paths := []string{"x", "y"}
// 最终结果
//  key | x | y
//  A  | 1 | 2
//  A  | 3 | 4
//  B  | 5 | 6
//  B  | 7 | 8

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

// 输入数据 ([]Person):
// [
//   {
//     Name: "Alice",
//     Age: 30,
//     Address: {
//       Street: "123 Main St",
//       City: "New York"
//     }
//   },
//   {
//     Name: "Bob",
//     Age: 25,
//     Address: {
//       Street: "456 Elm St",
//       City: "Chicago"
//     }
//   }
// ]

// paths: ["Name", "Age", "Address.Street", "Address.City"]

//        |
//        v

// FlexibleToDataFrame 处理
// (提取指定路径的数据并创建 DataFrame)

//        |
//        v

// 输出 DataFrame:
// +-------+-----+---------------+-----------+
// | Name  | Age | Address.Street| Address.City |
// +-------+-----+---------------+-----------+
// | Alice | 30  | 123 Main St   | New York  |
// | Bob   | 25  | 456 Elm St    | Chicago   |
// +-------+-----+---------------+-----------+

// 3  | "c" | true
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

// Input Data (Nested Slice)
//
//	|
//	v
//
// +------------------+
// |  [                |
// |    {              |
// |      topColumn: A |
// |      deepSlice: [ |
// |        {x: 1, y: 2},
// |        {x: 3, y: 4}
// |      ]            |
// |    },             |
// |    {              |
// |      topColumn: B |
// |      deepSlice: [ |
// |        {x: 5, y: 6},
// |        {x: 7, y: 8}
// |      ]            |
// |    }              |
// |  ]                |
// +------------------+
//
//	|
//	| DeepSliceToDataFrame
//	v
//
// +------------------+
// | DataFrame        |
// |                  |
// | topColumn | x | y |
// |    A      | 1 | 2 |
// |    A      | 3 | 4 |
// |    B      | 5 | 6 |
// |    B      | 7 | 8 |
// +------------------+
//
// topColumnPath := "topColumn"
// slicePath := "deepSlice"
// strictMode := true
// paths := []string{"x", "y"]
func DeepSliceToDataFrame(data interface{}, topColumnPath string, slicePath string, strictMode bool, paths ...string) (dataframe.DataFrame, error) {
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return dataframe.New(), fmt.Errorf("input must be a slice")
	}

	var resultDF dataframe.DataFrame
	topColumnValues := make([]interface{}, 0)
	allDeepSliceData := make([]interface{}, 0)

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()

		// Extract top column value
		topColumnValue, err := GetValueByPath(elem, topColumnPath)
		if err != nil {
			if strictMode {
				return dataframe.New(), fmt.Errorf("error extracting top column value at index %d: %v", i, err)
			}
			topColumnValue = nil
		}

		// Extract deep slice
		deepSliceValue, err := GetValueByPath(elem, slicePath)
		if err != nil {
			if strictMode {
				return dataframe.New(), fmt.Errorf("error extracting deep slice at index %d: %v", i, err)
			}
			continue
		}

		deepSlice := reflect.ValueOf(deepSliceValue)
		if deepSlice.Kind() != reflect.Slice {
			return dataframe.New(), fmt.Errorf("value at slicePath must be a slice")
		}

		// Process each item in the deep slice
		for j := 0; j < deepSlice.Len(); j++ {
			topColumnValues = append(topColumnValues, topColumnValue)
			allDeepSliceData = append(allDeepSliceData, deepSlice.Index(j).Interface())
		}
	}

	// Create DataFrame for deep slice data using FlexibleToDataFrame
	deepSliceDF, err := FlexibleToDataFrame(allDeepSliceData, strictMode, paths...)
	if err != nil {
		return dataframe.New(), fmt.Errorf("error creating DataFrame from deep slice data: %v", err)
	}

	// Add top column to the DataFrame
	topColumnSeries := series.New(topColumnValues, series.String, topColumnPath)
	resultDF = deepSliceDF.Mutate(topColumnSeries)

	// Reorder columns to put top column first
	newOrder := append([]string{topColumnPath}, deepSliceDF.Names()...)
	resultDF = resultDF.Select(newOrder)

	return resultDF, resultDF.Error()
}

func DeepSliceToSlice[T any](data interface{}, element T, slicePath string, strictMode bool, paths ...string) ([]T, error) {
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("input must be a slice")
	}

	result := make([]T, 0)

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()

		// Extract deep slice
		deepSliceValue, err := GetValueByPath(elem, slicePath)
		if err != nil {
			if strictMode {
				return nil, fmt.Errorf("error extracting deep slice at index %d: %v", i, err)
			}
			continue
		}

		deepSlice := reflect.ValueOf(deepSliceValue)
		if deepSlice.Kind() != reflect.Slice {
			return nil, fmt.Errorf("value at slicePath must be a slice")
		}

		// If paths is empty, deep copy the elements of the deep slice
		// 在 DeepSliceToSlice 函数中
		if len(paths) == 0 {
			for j := 0; j < deepSlice.Len(); j++ {
				item := deepSlice.Index(j).Interface()
				if reflect.TypeOf(item).AssignableTo(reflect.TypeOf(element)) {
					// 创建一个新的指针值
					newValue := reflect.New(reflect.TypeOf(element)).Interface()
					// 使用指针调用 DeepCopy
					err := DeepCopy(newValue, item)
					if err != nil {
						return nil, fmt.Errorf("error deep copying element at index %d,%d: %v", i, j, err)
					}
					// 将指针解引用并添加到结果中
					result = append(result, reflect.ValueOf(newValue).Elem().Interface().(T))
				} else {
					return nil, fmt.Errorf("element type mismatch at index %d,%d: expected %T, got %T", i, j, element, item)
				}
			}
		} else {
			// Process each item in the deep slice
			for j := 0; j < deepSlice.Len(); j++ {
				item := deepSlice.Index(j).Interface()
				newElement := reflect.New(reflect.TypeOf(element)).Elem()

				// Extract values for each path
				for _, path := range paths {
					value, err := GetValueByPath(item, path)
					if err != nil {
						if strictMode {
							return nil, fmt.Errorf("error extracting value from path %s for element %d,%d: %v", path, i, j, err)
						}
						value = nil
					}

					field := newElement.FieldByName(path)
					if field.IsValid() && field.CanSet() {
						err := setField(field, value)
						if err != nil {
							return nil, fmt.Errorf("error setting field %s: %v", path, err)
						}
					}
				}

				result = append(result, newElement.Interface().(T))
			}
		}
	}

	return result, nil // Always return the slice, even if it's empty
}

func DeepCopy(dst, src interface{}) error {
	dstVal := reflect.ValueOf(dst)
	srcVal := reflect.ValueOf(src)

	// Check if dst is a pointer and not nil
	if dstVal.Kind() != reflect.Ptr || dstVal.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	// Get the element that dst points to
	dstElem := dstVal.Elem()

	// If src is a pointer, get its element; otherwise use src directly
	if srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}

	// Check if types are compatible
	if !srcVal.Type().AssignableTo(dstElem.Type()) {
		return fmt.Errorf("source type %v is not assignable to destination type %v", srcVal.Type(), dstElem.Type())
	}

	// Perform the actual copy
	return deepCopy(dstElem, srcVal, make(map[uintptr]bool))
}

func deepCopy(dst, src reflect.Value, visited map[uintptr]bool) error {
	// 只有在处理可寻址的复杂类型时才检查和记录访问
	if src.Kind() == reflect.Ptr || src.Kind() == reflect.Interface || src.Kind() == reflect.Struct ||
		src.Kind() == reflect.Slice || src.Kind() == reflect.Map {
		if src.CanAddr() {
			ptr := src.UnsafeAddr()
			if visited[ptr] {
				return nil
			}
			visited[ptr] = true
		}
	}

	if !src.IsValid() {
		return fmt.Errorf("source value is invalid")
	}

	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}

	if src.Kind() == reflect.Ptr {
		if src.IsNil() {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		src = src.Elem()
	}

	if !src.Type().AssignableTo(dst.Type()) {
		return fmt.Errorf("types do not match: dst %v vs src %v", dst.Type(), src.Type())
	}

	switch src.Kind() {
	case reflect.String:
		if dst.CanSet() {
			dst.SetString(src.String())
		}
	case reflect.Struct:
		for i := 0; i < src.NumField(); i++ {
			if err := deepCopy(dst.Field(i), src.Field(i), visited); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if src.IsNil() {
			dst.Set(reflect.Zero(src.Type()))
			return nil
		}
		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Cap()))
		for i := 0; i < src.Len(); i++ {
			if err := deepCopy(dst.Index(i), src.Index(i), visited); err != nil {
				return err
			}
		}
	case reflect.Array:
		if dst.Len() != src.Len() {
			return fmt.Errorf("cannot copy array of different length")
		}
		for i := 0; i < src.Len(); i++ {
			if err := deepCopy(dst.Index(i), src.Index(i), visited); err != nil {
				return err
			}
		}
	case reflect.Map:
		if src.IsNil() {
			dst.Set(reflect.Zero(src.Type()))
			return nil
		}
		dst.Set(reflect.MakeMap(src.Type()))
		for _, key := range src.MapKeys() {
			dstVal := reflect.New(src.MapIndex(key).Type()).Elem()
			if err := deepCopy(dstVal, src.MapIndex(key), visited); err != nil {
				return err
			}
			dst.SetMapIndex(key, dstVal)
		}
	case reflect.Interface:
		if src.IsNil() {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		srcElem := src.Elem()
		dstElem := reflect.New(srcElem.Type()).Elem()
		if err := deepCopy(dstElem, srcElem, visited); err != nil {
			return err
		}
		dst.Set(dstElem)
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		if src.IsNil() {
			dst.Set(reflect.Zero(src.Type()))
			return nil
		}
		dst.Set(src)
	default:
		dst.Set(src)
	}

	return nil
}

// func DeepCopy(dst, src interface{}) error {
// 	if dst == nil {
// 		return errors.New("dst cannot be nil")
// 	}
// 	if src == nil {
// 		return nil
// 	}

// 	dstVal := reflect.ValueOf(dst)
// 	srcVal := reflect.ValueOf(src)

// 	if dstVal.Kind() != reflect.Ptr {
// 		return errors.New("dst must be a pointer")
// 	}

// 	dstVal = dstVal.Elem()

// 	if srcVal.Kind() == reflect.Ptr {
// 		srcVal = srcVal.Elem()
// 	}

// 	if !srcVal.Type().AssignableTo(dstVal.Type()) {
// 		return fmt.Errorf("cannot assign %v to %v", srcVal.Type(), dstVal.Type())
// 	}

// 	visited := make(map[uintptr]reflect.Value)
// 	return deepCopyValue(dstVal, srcVal, visited)
// }

// func deepCopyValue(dst, src reflect.Value, visited map[uintptr]reflect.Value) error {
// 	if !src.IsValid() {
// 		return nil
// 	}

// 	if src.CanAddr() {
// 		ptr := src.UnsafeAddr()
// 		if v, ok := visited[ptr]; ok {
// 			dst.Set(v)
// 			return nil
// 		}
// 		visited[ptr] = dst
// 	}

// 	switch src.Kind() {
// 	case reflect.Struct:
// 		for i := 0; i < src.NumField(); i++ {
// 			if err := deepCopyValue(dst.Field(i), src.Field(i), visited); err != nil {
// 				return err
// 			}
// 		}
// 	case reflect.Slice:
// 		if src.IsNil() {
// 			dst.Set(reflect.Zero(src.Type()))
// 			return nil
// 		}
// 		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Cap()))
// 		for i := 0; i < src.Len(); i++ {
// 			if err := deepCopyValue(dst.Index(i), src.Index(i), visited); err != nil {
// 				return err
// 			}
// 		}
// 	case reflect.Array:
// 		for i := 0; i < src.Len(); i++ {
// 			if err := deepCopyValue(dst.Index(i), src.Index(i), visited); err != nil {
// 				return err
// 			}
// 		}
// 	case reflect.Map:
// 		if src.IsNil() {
// 			dst.Set(reflect.Zero(src.Type()))
// 			return nil
// 		}
// 		dst.Set(reflect.MakeMap(src.Type()))
// 		for _, key := range src.MapKeys() {
// 			dstVal := reflect.New(src.MapIndex(key).Type()).Elem()
// 			if err := deepCopyValue(dstVal, src.MapIndex(key), visited); err != nil {
// 				return err
// 			}
// 			dst.SetMapIndex(key, dstVal)
// 		}
// 	case reflect.Ptr:
// 		if src.IsNil() {
// 			dst.Set(reflect.Zero(dst.Type()))
// 			return nil
// 		}
// 		dst.Set(reflect.New(src.Elem().Type()))
// 		return deepCopyValue(dst.Elem(), src.Elem(), visited)
// 	case reflect.Interface:
// 		if src.IsNil() {
// 			dst.Set(reflect.Zero(dst.Type()))
// 			return nil
// 		}
// 		srcElem := src.Elem()
// 		dstElem := reflect.New(srcElem.Type()).Elem()
// 		if err := deepCopyValue(dstElem, srcElem, visited); err != nil {
// 			return err
// 		}
// 		dst.Set(dstElem)
// 	default:
// 		dst.Set(src)
// 	}
// 	return nil
// }

// func DeepCopy(dst, src interface{}) error {
// 	dstVal := reflect.ValueOf(dst)
// 	srcVal := reflect.ValueOf(src)

// 	// Check if dst is a pointer and not nil
// 	if dstVal.Kind() != reflect.Ptr || dstVal.IsNil() {
// 		return fmt.Errorf("destination must be a non-nil pointer")
// 	}

// 	// Get the element that dst points to
// 	dstElem := dstVal.Elem()

// 	// If src is a pointer, get its element; otherwise use src directly
// 	if srcVal.Kind() == reflect.Ptr {
// 		srcVal = srcVal.Elem()
// 	}

// 	// Check if types are compatible
// 	if !srcVal.Type().AssignableTo(dstElem.Type()) {
// 		return fmt.Errorf("source type %v is not assignable to destination type %v", srcVal.Type(), dstElem.Type())
// 	}

// 	// Perform the actual copy
// 	return deepCopy(dstElem, srcVal, make(map[uintptr]interface{}))
// }
// func deepCopy(dst, src reflect.Value, visited map[uintptr]interface{}) error {
// 	if !src.IsValid() {
// 		return nil
// 	}

// 	if src.CanAddr() {
// 		ptr := src.Addr().Pointer()
// 		if v, ok := visited[ptr]; ok {
// 			if dst.CanSet() {
// 				dst.Set(reflect.ValueOf(v).Elem())
// 			}
// 			return nil
// 		}
// 		visited[ptr] = dst.Addr().Interface()
// 	}

// 	switch src.Kind() {
// 	case reflect.Ptr:
// 		if src.IsNil() {
// 			return nil
// 		}
// 		dst.Set(reflect.New(src.Elem().Type()))
// 		return deepCopy(dst.Elem(), src.Elem(), visited)
// 	case reflect.Interface:
// 		if src.IsNil() {
// 			return nil
// 		}
// 		srcElem := src.Elem()
// 		dstElem := reflect.New(srcElem.Type()).Elem()
// 		if err := deepCopy(dstElem, srcElem, visited); err != nil {
// 			return err
// 		}
// 		dst.Set(dstElem)
// 	case reflect.Struct:
// 		for i := 0; i < src.NumField(); i++ {
// 			if err := deepCopy(dst.Field(i), src.Field(i), visited); err != nil {
// 				return err
// 			}
// 		}
// 	case reflect.Slice:
// 		if src.IsNil() {
// 			return nil
// 		}
// 		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Cap()))
// 		for i := 0; i < src.Len(); i++ {
// 			if err := deepCopy(dst.Index(i), src.Index(i), visited); err != nil {
// 				return err
// 			}
// 		}
// 	case reflect.Map:
// 		if src.IsNil() {
// 			return nil
// 		}
// 		dst.Set(reflect.MakeMap(src.Type()))
// 		for _, key := range src.MapKeys() {
// 			dstVal := reflect.New(src.MapIndex(key).Type()).Elem()
// 			if err := deepCopy(dstVal, src.MapIndex(key), visited); err != nil {
// 				return err
// 			}
// 			dst.SetMapIndex(key, dstVal)
// 		}
// 	default:
// 		if dst.CanSet() && src.Type().AssignableTo(dst.Type()) {
// 			dst.Set(src)
// 		} else {
// 			return fmt.Errorf("cannot copy %v to %v", src.Type(), dst.Type())
// 		}
// 	}
// 	return nil
// }

func DataframeToStruct[T any](df dataframe.DataFrame) ([]T, error) {
	var result []T

	// Get the type of T
	t := reflect.TypeOf((*T)(nil)).Elem()

	// Check if T is a struct
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("T must be a struct type")
	}

	// Create a map of JSON tag to field index and track required fields
	tagToField := make(map[string]int)
	requiredFields := make(map[string]bool)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		if tag != "" {
			tagParts := strings.Split(tag, ",")
			tagToField[tagParts[0]] = i
			if field.Tag.Get("required") == "true" {
				requiredFields[tagParts[0]] = true
			}
		}
	}

	// Get DataFrame column names
	dfColumns := df.Names()

	// Iterate over each row in the DataFrame
	for i := 0; i < df.Nrow(); i++ {
		// Create a new instance of T
		newStruct := reflect.New(t).Elem()

		// Get the row data
		_, row := df.Row(i)

		missingRequiredFields := []string{}

		// Iterate over each JSON tag
		for tag, fieldIndex := range tagToField {
			// Check if the column exists in the DataFrame
			if !contains(dfColumns, tag) {
				if requiredFields[tag] {
					missingRequiredFields = append(missingRequiredFields, tag)
				}
				continue // Skip this field if it's not in the DataFrame
			}

			// Get the value from the DataFrame row
			value := row[tag]

			// Set the value in the struct field
			structField := newStruct.Field(fieldIndex)
			if structField.CanSet() {
				err := setField(structField, value)
				if err != nil {
					return nil, fmt.Errorf("error setting field for tag '%s' at row %d: %v", tag, i, err)
				}
			}
		}

		if len(missingRequiredFields) > 0 {
			return nil, fmt.Errorf("missing required fields at row %d: %v", i, missingRequiredFields)
		}

		// Append the new struct to the result slice
		result = append(result, newStruct.Interface().(T))
	}

	return result, nil
}

// Helper function to set a struct field value
func setField(field reflect.Value, value interface{}) error {
	if value == nil {
		return nil // Skip nil values
	}

	v := reflect.ValueOf(value)

	// Handle type conversions
	switch field.Kind() {
	case reflect.String:
		field.SetString(fmt.Sprintf("%v", value))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intVal, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
		if err != nil {
			return err
		}
		field.SetInt(intVal)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uintVal, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
		if err != nil {
			return err
		}
		field.SetUint(uintVal)
	case reflect.Float32, reflect.Float64:
		floatVal, err := strconv.ParseFloat(fmt.Sprintf("%v", value), 64)
		if err != nil {
			return err
		}
		field.SetFloat(floatVal)
	case reflect.Bool:
		boolVal, err := strconv.ParseBool(fmt.Sprintf("%v", value))
		if err != nil {
			return err
		}
		field.SetBool(boolVal)
	default:
		if field.Type() == v.Type() {
			field.Set(v)
		} else {
			return fmt.Errorf("incompatible types: %v and %v", field.Type(), v.Type())
		}
	}

	return nil
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
		if v == nil {
			continue
		}
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				continue
			}
			rv = rv.Elem()
		}
		switch rv.Kind() {
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

	for i, v := range data {
		if v == nil {
			newData[i] = nil
			continue
		}
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				newData[i] = nil
				continue
			}
			rv = rv.Elem()
		}

		switch t {
		case series.Int:
			switch rv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				newData[i] = int(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				newData[i] = int(rv.Uint())
			default:
				newData[i] = nil
			}
		case series.Float:
			if rv.Kind() == reflect.Float64 {
				newData[i] = rv.Float()
			} else if rv.Kind() == reflect.Float32 {
				newData[i] = float64(rv.Float())
			} else {
				newData[i] = nil
			}
		case series.Bool:
			if rv.Kind() == reflect.Bool {
				newData[i] = rv.Bool()
			} else {
				newData[i] = nil
			}
		default:
			switch rv.Kind() {
			case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
				newData[i] = toJSON(rv.Interface())
			default:
				newData[i] = fmt.Sprintf("%v", rv.Interface())
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

// Helper function to check if a slice contains a string
func contains(slice []string, str string) bool {
	for _, v := range slice {
		if v == str {
			return true
		}
	}
	return false
}
