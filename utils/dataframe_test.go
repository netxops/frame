package utils

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/netxops/frame/dataframe"
	"github.com/netxops/frame/series"
	"github.com/stretchr/testify/assert"
)

func TestCreateSeriesFromData(t *testing.T) {
	t.Run("Integer pointers", func(t *testing.T) {
		// Create integer pointers
		i1, i2, i3 := 1, 2, 3
		data := []interface{}{&i1, &i2, &i3}

		s, err := createSeriesFromData(data, "int_pointers")

		assert.NoError(t, err)
		assert.Equal(t, series.Int, s.Type())
		assert.Equal(t, "int_pointers", s.Name)
		ints, _ := s.Int()
		assert.Equal(t, []int{1, 2, 3}, ints)
	})

	t.Run("Float pointers", func(t *testing.T) {
		// Create float pointers
		f1, f2, f3 := 1.1, 2.2, 3.3
		data := []interface{}{&f1, &f2, &f3}

		s, err := createSeriesFromData(data, "float_pointers")

		assert.NoError(t, err)
		assert.Equal(t, series.Float, s.Type())
		assert.Equal(t, "float_pointers", s.Name)
		assert.Equal(t, []float64{1.1, 2.2, 3.3}, s.Float())
	})

	t.Run("Bool pointers", func(t *testing.T) {
		// Create bool pointers
		b1, b2, b3 := true, false, true
		data := []interface{}{&b1, &b2, &b3}

		s, err := createSeriesFromData(data, "bool_pointers")

		assert.NoError(t, err)
		assert.Equal(t, series.Bool, s.Type())
		assert.Equal(t, "bool_pointers", s.Name)
		bools, _ := s.Bool()
		assert.Equal(t, []bool{true, false, true}, bools)
	})

	t.Run("String pointers", func(t *testing.T) {
		// Create string pointers
		s1, s2, s3 := "a", "b", "c"
		data := []interface{}{&s1, &s2, &s3}

		s, err := createSeriesFromData(data, "string_pointers")

		assert.NoError(t, err)
		assert.Equal(t, series.String, s.Type())
		assert.Equal(t, "string_pointers", s.Name)
		assert.Equal(t, []string{"a", "b", "c"}, s.Records())
	})

	t.Run("Mixed pointers and values", func(t *testing.T) {
		// Mix of pointers and direct values
		i1, f1, b1 := 1, 2.2, true
		data := []interface{}{&i1, f1, &b1}

		s, err := createSeriesFromData(data, "mixed_data")

		assert.NoError(t, err)
		// The type should be determined by the first non-nil element (int in this case)
		assert.Equal(t, series.Int, s.Type())
		assert.Equal(t, "mixed_data", s.Name)
		// Non-int values should be nil
		expectedData := []string{"1", "NaN", "NaN"}
		assert.Equal(t, expectedData, s.Records())
	})

	t.Run("Nil pointers", func(t *testing.T) {
		// Create a slice with some nil pointers
		var ip *int = nil
		i1 := 42
		data := []interface{}{ip, &i1, nil}

		s, err := createSeriesFromData(data, "nil_pointers")

		assert.NoError(t, err)
		assert.Equal(t, series.Int, s.Type())
		assert.Equal(t, "nil_pointers", s.Name)
		expectedData := []string{"NaN", "42", "NaN"}
		assert.Equal(t, expectedData, s.Records())
	})

	t.Run("Struct pointers", func(t *testing.T) {
		// Create struct and struct pointers
		type TestStruct struct {
			Field1 string
			Field2 int
		}

		s1 := TestStruct{Field1: "test1", Field2: 1}
		s2 := TestStruct{Field1: "test2", Field2: 2}
		data := []interface{}{&s1, s2}

		s, err := createSeriesFromData(data, "struct_pointers")

		assert.NoError(t, err)
		assert.Equal(t, series.String, s.Type())
		assert.Equal(t, "struct_pointers", s.Name)

		// Both should be converted to JSON strings
		records := s.Records()
		assert.Len(t, records, 2)
		assert.Contains(t, records[0], "test1")
		assert.Contains(t, records[1], "test2")
	})

	t.Run("Empty data", func(t *testing.T) {
		data := []interface{}{}

		_, err := createSeriesFromData(data, "empty_data")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "data is empty")
	})
}

func TestGetValueByPathNestedStruct(t *testing.T) {
	type Address struct {
		Street string
		City   string
	}
	type Person struct {
		Name    string
		Age     int
		Address Address
	}

	data := Person{
		Name: "Alice",
		Age:  30,
		Address: Address{
			Street: "123 Main St",
			City:   "New York",
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		hasError bool
	}{
		{"Top-level field", "Name", "Alice", false},
		{"Nested field", "Address.Street", "123 Main St", false},
		{"Non-existent field", "Address.Country", nil, true},
		{"Invalid path", "Address.Street.Number", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetValueByPath(data, tt.path)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetValueByPathMapAccess(t *testing.T) {
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "Alice",
			"age":  30,
			"address": map[string]string{
				"city":    "New York",
				"country": "USA",
			},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		hasError bool
	}{
		{"Access nested map value", "user.name", "Alice", false},
		{"Access deeply nested map value", "user.address.city", "New York", false},
		{"Access non-existent key", "user.email", nil, true},
		{"Access non-existent nested key", "user.address.street", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetValueByPath(data, tt.path)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetValueByPathSliceAccess(t *testing.T) {
	data := map[string]interface{}{
		"users": []map[string]interface{}{
			{"name": "Alice", "age": 30},
			{"name": "Bob", "age": 25},
			{"name": "Charlie", "age": 35},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		hasError bool
	}{
		{"Access first user's name", "users.0.name", "Alice", false},
		{"Access second user's age", "users.1.age", 25, false},
		{"Access non-existent index", "users.3.name", nil, true},
		{"Invalid index", "users.abc.name", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetValueByPath(data, tt.path)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetValueByPathNonExistentField(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	data := Person{
		Name: "Alice",
		Age:  30,
	}

	result, err := GetValueByPath(data, "Address")

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, "field not found: Address", err.Error())
}

func TestGetValueByPathNilInput(t *testing.T) {
	result, err := GetValueByPath(nil, "some.path")

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, "invalid value encountered at key: some", err.Error())
}

func TestGetValueByPathEmptyPath(t *testing.T) {
	data := struct {
		Name string
		Age  int
	}{
		Name: "Alice",
		Age:  30,
	}

	result, err := GetValueByPath(data, "")

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, "empty path is not allowed", err.Error())
}

func TestGetValueByPathPointerToStruct(t *testing.T) {
	type Address struct {
		Street string
		City   string
	}
	type Person struct {
		Name    string
		Age     int
		Address *Address
	}

	data := &Person{
		Name: "Alice",
		Age:  30,
		Address: &Address{
			Street: "123 Main St",
			City:   "New York",
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		hasError bool
	}{
		{"Top-level field", "Name", "Alice", false},
		{"Nested pointer field", "Address.Street", "123 Main St", false},
		{"Non-existent field", "Address.Country", nil, true},
		{"Invalid path", "Address.Street.Number", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetValueByPath(data, tt.path)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetValueByPathNilPointer(t *testing.T) {
	type Person struct {
		Name    string
		Address *struct {
			City string
		}
	}

	data := Person{
		Name:    "Alice",
		Address: nil,
	}

	result, err := GetValueByPath(data, "Address.City")

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, "nil pointer encountered at key: City", err.Error())
}

func TestGetValueByPathInterfaceWithSlice(t *testing.T) {
	data := map[string]interface{}{
		"users": []interface{}{
			map[string]interface{}{
				"name": "Alice",
				"age":  30,
			},
			map[string]interface{}{
				"name": "Bob",
				"age":  25,
			},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		hasError bool
	}{
		{"Access first user's name", "users.0.name", "Alice", false},
		{"Access second user's age", "users.1.age", 25, false},
		{"Access non-existent index", "users.2.name", nil, true},
		{"Invalid index", "users.abc.name", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetValueByPath(data, tt.path)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// func TestGetValueByPathInterfaceUnsupportedType(t *testing.T) {
// 	type UnsupportedType struct {
// 		Value int
// 	}

// 	data := map[string]interface{}{
// 		"key": interface{}(UnsupportedType{Value: 42}),
// 	}

// 	result, err := GetValueByPath(data, "key.Value")

// 	assert.Nil(t, result)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "unsupported type after interface")
// }

func TestGetValueByPathArrayAccess(t *testing.T) {
	data := map[string]interface{}{
		"users": []interface{}{
			map[string]interface{}{"name": "Alice", "age": 30},
			map[string]interface{}{"name": "Bob", "age": 25},
			map[string]interface{}{"name": "Charlie", "age": 35},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		hasError bool
	}{
		{"Access first user's name", "users.0.name", "Alice", false},
		{"Access second user's age", "users.1.age", 25, false},
		{"Access third user's name", "users.2.name", "Charlie", false},
		{"Access non-existent index", "users.3.name", nil, true},
		{"Invalid index", "users.abc.name", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetValueByPath(data, tt.path)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetValueByPathOutOfBoundsArrayAccess(t *testing.T) {
	data := map[string]interface{}{
		"users": []string{"Alice", "Bob", "Charlie"},
	}

	result, err := GetValueByPath(data, "users.3")

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, "array index out of bounds at key: 3", err.Error())
}

func TestGetValueByPathNestedPointerDereference(t *testing.T) {
	type InnerStruct struct {
		Value string
	}
	type MiddleStruct struct {
		Inner *InnerStruct
	}
	type OuterStruct struct {
		Middle *MiddleStruct
	}

	data := &OuterStruct{
		Middle: &MiddleStruct{
			Inner: &InnerStruct{
				Value: "nested value",
			},
		},
	}

	result, err := GetValueByPath(data, "Middle.Inner.Value")

	assert.NoError(t, err)
	assert.Equal(t, "nested value", result)

	// Test with a nil pointer in the middle
	nilData := &OuterStruct{
		Middle: nil,
	}

	result, err = GetValueByPath(nilData, "Middle.Inner.Value")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, "nil pointer encountered at key: Inner", err.Error())
}

func TestGetValueByPathUnsupportedTypeAtEnd(t *testing.T) {
	data := struct {
		Name string
		Age  int
		Func func()
	}{
		Name: "Alice",
		Age:  30,
		Func: func() {},
	}

	result, err := GetValueByPath(data, "Func")

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, "unsupported type: func at key: Func", err.Error())
}

func TestFlexibleToDataFrameNonSliceInput(t *testing.T) {
	// Prepare non-slice input
	data := map[string]string{"key": "value"}

	// Call the function with non-slice input
	_, err := FlexibleToDataFrame(data, true, "key")

	// Assert that an error is returned
	assert.Error(t, err)
	assert.Equal(t, "input must be a slice", err.Error())
}

func TestFlexibleToDataFrameEmptySlice(t *testing.T) {
	// Prepare an empty slice input
	data := []interface{}{}
	paths := []string{"column1", "column2", "column3"}

	// Call the function with empty slice input
	df, err := FlexibleToDataFrame(data, true, paths...)

	// Assert that no error is returned
	assert.NoError(t, err)

	// Assert that the DataFrame is not nil
	assert.NotNil(t, df)

	// Assert that the DataFrame has the correct number of columns
	assert.Equal(t, len(paths), df.Ncol())

	// Assert that each column is an empty series with the correct name
	for _, path := range paths {
		s := df.Col(path)
		assert.NotNil(t, s)
		assert.Equal(t, path, s.Name)
		assert.Equal(t, 0, s.Len())
		assert.Equal(t, series.String, s.Type())
	}
}

func TestFlexibleToDataFrameMultiplePaths(t *testing.T) {
	type Person struct {
		Name    string
		Age     int
		Address struct {
			City    string
			Country string
		}
	}

	data := []Person{
		{Name: "Alice", Age: 30, Address: struct {
			City    string
			Country string
		}{City: "New York", Country: "USA"}},
		{Name: "Bob", Age: 25, Address: struct {
			City    string
			Country string
		}{City: "London", Country: "UK"}},
	}

	df, err := FlexibleToDataFrame(data, false, "Name", "Age", "Address.City")

	assert.NoError(t, err)
	assert.Equal(t, 3, df.Ncol())
	assert.Equal(t, 2, df.Nrow())

	nameSeries := df.Col("Name")
	ageSeries := df.Col("Age")
	citySeries := df.Col("Address.City")

	assert.Equal(t, []string{"Alice", "Bob"}, nameSeries.Records())
	ages, _ := ageSeries.Int()
	assert.Equal(t, []int{30, 25}, ages)
	assert.Equal(t, []string{"New York", "London"}, citySeries.Records())
}

func TestFlexibleToDataFrameCreateSeriesFromPathError(t *testing.T) {
	// Mock data that will cause createSeriesFromPath to fail
	mockData := []interface{}{
		map[string]interface{}{"name": "Alice"},
		map[string]interface{}{"name": "Bob"},
	}

	// Call FlexibleToDataFrame with a path that doesn't exist
	_, err := FlexibleToDataFrame(mockData, true, "nonexistent.path")

	// Assert that an error is returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error extracting value from path nonexistent.path")
}

func TestFlexibleToDataFrameNestedStructPaths(t *testing.T) {
	type Address struct {
		Street string
		City   string
	}
	type Person struct {
		Name    string
		Age     int
		Address Address
	}

	data := []Person{
		{Name: "Alice", Age: 30, Address: Address{Street: "123 Main St", City: "New York"}},
		{Name: "Bob", Age: 25, Address: Address{Street: "456 Elm St", City: "Chicago"}},
	}

	df, err := FlexibleToDataFrame(data, true, "Name", "Age", "Address.Street", "Address.City")

	assert.NoError(t, err)
	assert.Equal(t, 4, df.Ncol())
	assert.Equal(t, 2, df.Nrow())

	assert.Equal(t, []string{"Alice", "Bob"}, df.Col("Name").Records())
	ages, _ := df.Col("Age").Int()
	assert.Equal(t, []int{30, 25}, ages)
	assert.Equal(t, []string{"123 Main St", "456 Elm St"}, df.Col("Address.Street").Records())
	assert.Equal(t, []string{"New York", "Chicago"}, df.Col("Address.City").Records())
}

func TestFlexibleToDataFrameSliceOfMapsWithVaryingStructures(t *testing.T) {
	data := []map[string]interface{}{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "city": "London", "job": "Engineer"},
	}

	df, err := FlexibleToDataFrame(data, false, "name", "age", "city", "job")

	assert.NoError(t, err)
	assert.Equal(t, 4, df.Ncol())
	assert.Equal(t, 3, df.Nrow())

	expectedNames := []string{"Alice", "Bob", "Charlie"}
	expectedAges := []string{"30", "25", "NaN"}
	expectedCities := []string{"New York", "NaN", "London"}
	expectedJobs := []string{"NaN", "NaN", "Engineer"}

	assert.Equal(t, expectedNames, df.Col("name").Records())
	assert.Equal(t, expectedAges, df.Col("age").Records())
	assert.Equal(t, expectedCities, df.Col("city").Records())
	assert.Equal(t, expectedJobs, df.Col("job").Records())
}

func TestFlexibleToDataFrameInvalidPaths(t *testing.T) {
	data := []map[string]interface{}{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	_, err := FlexibleToDataFrame(data, true, "name", "invalid_path")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error extracting value from path invalid_path")
}

func TestFlexibleToDataFrameDifferentDataTypes(t *testing.T) {
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice", "score": 85.5, "passed": true},
		{"id": 2, "name": "Bob", "score": 92.0, "passed": true},
		{"id": 3, "name": "Charlie", "score": 78.5, "passed": false},
	}

	df, err := FlexibleToDataFrame(data, true, "id", "name", "score", "passed")

	assert.NoError(t, err)
	assert.Equal(t, 4, df.Ncol())
	assert.Equal(t, 3, df.Nrow())

	idSeries := df.Col("id")
	nameSeries := df.Col("name")
	scoreSeries := df.Col("score")
	passedSeries := df.Col("passed")

	assert.Equal(t, series.Int, idSeries.Type())
	assert.Equal(t, series.String, nameSeries.Type())
	assert.Equal(t, series.Float, scoreSeries.Type())
	assert.Equal(t, series.Bool, passedSeries.Type())
	ids, _ := idSeries.Int()
	assert.Equal(t, []int{1, 2, 3}, ids)
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, nameSeries.Records())
	assert.Equal(t, []float64{85.5, 92.0, 78.5}, scoreSeries.Float())
	passedList, _ := passedSeries.Bool()
	assert.Equal(t, []bool{true, true, false}, passedList)
}

func TestFlexibleToDataFrameMaintainSeriesOrder(t *testing.T) {
	data := []map[string]interface{}{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "London"},
		{"name": "Charlie", "age": 35, "city": "Paris"},
	}

	paths := []string{"city", "name", "age"}

	df, err := FlexibleToDataFrame(data, true, paths...)

	assert.NoError(t, err)
	assert.NotNil(t, df)

	expectedColumns := []string{"city", "name", "age"}
	actualColumns := df.Names()

	assert.Equal(t, expectedColumns, actualColumns, "The order of series in the DataFrame should match the order of paths")

	// Verify the content of each series
	cityS := df.Col("city")
	nameS := df.Col("name")
	ageS := df.Col("age")

	assert.Equal(t, []string{"New York", "London", "Paris"}, cityS.Records())
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, nameS.Records())
	ages, _ := ageS.Int()
	assert.Equal(t, []int{30, 25, 35}, ages)
}

func TestFlexibleToDataFrameLargeDataset(t *testing.T) {
	// Create a large dataset
	size := 1000000
	data := make([]map[string]interface{}, size)
	for i := 0; i < size; i++ {
		data[i] = map[string]interface{}{
			"id":    i,
			"name":  fmt.Sprintf("Item %d", i),
			"value": float64(i) * 1.5,
		}
	}

	// Define paths
	paths := []string{"id", "name", "value"}

	// Measure execution time
	start := time.Now()
	df, err := FlexibleToDataFrame(data, true, paths...)
	duration := time.Since(start)

	// Assert no error
	assert.NoError(t, err)

	// Assert correct number of rows and columns
	assert.Equal(t, size, df.Nrow())
	assert.Equal(t, len(paths), df.Ncol())

	// Assert correct column names
	assert.Equal(t, paths, df.Names())

	// Check execution time (adjust threshold as needed)
	// assert.(t, duration, 5*time.Second, "Function took too long to execute")
	if duration > 5*time.Second {
		t.Errorf("Function took too long to execute: %v", duration)
	}

	// Spot check some values
	value, _ := df.Elem(0, 0).Int()
	assert.Equal(t, 0, value)
	assert.Equal(t, "Item 0", df.Elem(0, 1).String())
	assert.InDelta(t, 0.0, df.Elem(0, 2).Float(), 0.001)
	value, _ = df.Elem(size-1, 0).Int()
	assert.Equal(t, size-1, value)
	assert.Equal(t, fmt.Sprintf("Item %d", size-1), df.Elem(size-1, 1).String())
	assert.InDelta(t, float64(size-1)*1.5, df.Elem(size-1, 2).Float(), 0.001)

	// Check that the original data hasn't been modified
	assert.Equal(t, 0, data[0]["id"])
	assert.Equal(t, "Item 0", data[0]["name"])
	assert.Equal(t, 0.0, data[0]["value"])
	assert.Equal(t, size-1, data[size-1]["id"])
	assert.Equal(t, fmt.Sprintf("Item %d", size-1), data[size-1]["name"])
	assert.Equal(t, float64(size-1)*1.5, data[size-1]["value"])
}

func TestFlexibleToDataFrameTypeConsistency(t *testing.T) {
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice", "score": 85.5},
		{"id": 2, "name": "Bob", "score": 92.0},
		{"id": 3, "name": "Charlie", "score": 78.5},
	}

	df, err := FlexibleToDataFrame(data, true, "id", "name", "score")

	assert.NoError(t, err)
	assert.Equal(t, 3, df.Ncol())
	assert.Equal(t, 3, df.Nrow())

	idSeries := df.Col("id")
	nameSeries := df.Col("name")
	scoreSeries := df.Col("score")

	// Check types
	assert.Equal(t, series.Int, idSeries.Type())
	assert.Equal(t, series.String, nameSeries.Type())
	assert.Equal(t, series.Float, scoreSeries.Type())

	// Check values
	ids, _ := idSeries.Int()
	assert.Equal(t, []int{1, 2, 3}, ids)
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, nameSeries.Records())
	scores := scoreSeries.Float()
	assert.InDeltaSlice(t, []float64{85.5, 92.0, 78.5}, scores, 0.001)

	// Check that the original data hasn't been modified
	assert.Equal(t, 1, data[0]["id"])
	assert.Equal(t, "Alice", data[0]["name"])
	assert.Equal(t, 85.5, data[0]["score"])
}

func TestFlexibleToDataFrameMixedTypes(t *testing.T) {
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice", "score": 85.5, "passed": true},
		{"id": "2", "name": "Bob", "score": "92.0", "passed": "true"},
		{"id": 3.0, "name": "Charlie", "score": 78, "passed": false},
	}

	df, err := FlexibleToDataFrame(data, true, "id", "name", "score", "passed")

	assert.NoError(t, err)
	assert.Equal(t, 4, df.Ncol())
	assert.Equal(t, 3, df.Nrow())

	idSeries := df.Col("id")
	nameSeries := df.Col("name")
	scoreSeries := df.Col("score")
	passedSeries := df.Col("passed")

	// Check types
	assert.Equal(t, series.Int, idSeries.Type())
	assert.Equal(t, series.String, nameSeries.Type())
	assert.Equal(t, series.Float, scoreSeries.Type())
	assert.Equal(t, series.Bool, passedSeries.Type())

	// Check values
	ids := idSeries.Float()
	assert.InDeltaSlice(t, []float64{1}, ids[0:1], 0.001)
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, nameSeries.Records())
	scores := scoreSeries.Float()
	assert.InDeltaSlice(t, []float64{85.5}, scores[0:1], 0.001)
	// passed, _ := passedSeries.Bool()
	// assert.Equal(t, []bool{true, true, false}, passed)

	// Check that the original data hasn't been modified
	assert.Equal(t, 1, data[0]["id"])
	assert.Equal(t, "2", data[1]["id"])
	assert.Equal(t, 3.0, data[2]["id"])
	assert.Equal(t, "92.0", data[1]["score"])
	assert.Equal(t, 78, data[2]["score"])
	assert.Equal(t, "true", data[1]["passed"])
}

func TestFlexibleToDataFrameNestedKeys(t *testing.T) {
	data := []map[string]interface{}{
		{
			"id": 1,
			"personal": map[string]interface{}{
				"name": "Alice",
				"age":  30,
			},
			"professional": map[string]interface{}{
				"job": "Engineer",
				"skills": []string{
					"Go", "Python",
				},
			},
			"address": map[string]interface{}{
				"city": "New York",
				"country": map[string]string{
					"name": "USA",
					"code": "US",
				},
			},
		},
		{
			"id": 2,
			"personal": map[string]interface{}{
				"name": "Bob",
				"age":  25,
			},
			"professional": map[string]interface{}{
				"job": "Designer",
				"skills": []string{
					"Photoshop", "Illustrator",
				},
			},
			"address": map[string]interface{}{
				"city": "London",
				"country": map[string]string{
					"name": "UK",
					"code": "GB",
				},
			},
		},
	}

	paths := []string{
		"id",
		"personal.name",
		"personal.age",
		"professional.job",
		"professional.skills.0",
		"address.city",
		"address.country.name",
		"address.country.code",
	}

	df, err := FlexibleToDataFrame(data, true, paths...)

	assert.NoError(t, err)
	assert.Equal(t, len(paths), df.Ncol())
	assert.Equal(t, 2, df.Nrow())

	// Check column names
	assert.Equal(t, paths, df.Names())

	// Check values
	assert.Equal(t, []string{"1", "2"}, df.Col("id").Records())
	assert.Equal(t, []string{"Alice", "Bob"}, df.Col("personal.name").Records())
	assert.Equal(t, []string{"30", "25"}, df.Col("personal.age").Records())
	assert.Equal(t, []string{"Engineer", "Designer"}, df.Col("professional.job").Records())
	assert.Equal(t, []string{"Go", "Photoshop"}, df.Col("professional.skills.0").Records())
	assert.Equal(t, []string{"New York", "London"}, df.Col("address.city").Records())
	assert.Equal(t, []string{"USA", "UK"}, df.Col("address.country.name").Records())
	assert.Equal(t, []string{"US", "GB"}, df.Col("address.country.code").Records())

	// Check that the original data hasn't been modified
	assert.Equal(t, 1, data[0]["id"])
	assert.Equal(t, "Alice", data[0]["personal"].(map[string]interface{})["name"])
	assert.Equal(t, 30, data[0]["personal"].(map[string]interface{})["age"])
	assert.Equal(t, "Engineer", data[0]["professional"].(map[string]interface{})["job"])
	assert.Equal(t, []string{"Go", "Python"}, data[0]["professional"].(map[string]interface{})["skills"])
	assert.Equal(t, "New York", data[0]["address"].(map[string]interface{})["city"])
	assert.Equal(t, "USA", data[0]["address"].(map[string]interface{})["country"].(map[string]string)["name"])
	assert.Equal(t, "US", data[0]["address"].(map[string]interface{})["country"].(map[string]string)["code"])
}

func TestFlexibleToDataFrameFourLevelNestedKeys(t *testing.T) {
	data := []map[string]interface{}{
		{
			"id": 1,
			"personal": map[string]interface{}{
				"name": "Alice",
				"details": map[string]interface{}{
					"age": 30,
					"contact": map[string]interface{}{
						"email": "alice@example.com",
						"phone": map[string]string{
							"home": "123-456-7890",
							"work": "098-765-4321",
						},
					},
				},
			},
			"professional": map[string]interface{}{
				"job":    "Engineer",
				"skills": []string{"Go", "Python"},
				"projects": []map[string]interface{}{
					{
						"name": "Project A",
						"details": map[string]interface{}{
							"status": "Completed",
							"duration": map[string]int{
								"months": 6,
								"days":   15,
							},
						},
					},
				},
			},
		},
		{
			"id": 2,
			"personal": map[string]interface{}{
				"name": "Bob",
				"details": map[string]interface{}{
					"age": 25,
					"contact": map[string]interface{}{
						"email": "bob@example.com",
						"phone": map[string]string{
							"home": "111-222-3333",
							"work": "444-555-6666",
						},
					},
				},
			},
			"professional": map[string]interface{}{
				"job":    "Designer",
				"skills": []string{"Photoshop", "Illustrator"},
				"projects": []map[string]interface{}{
					{
						"name": "Project B",
						"details": map[string]interface{}{
							"status": "In Progress",
							"duration": map[string]int{
								"months": 3,
								"days":   20,
							},
						},
					},
				},
			},
		},
	}

	paths := []string{
		"id",
		"personal.name",
		"personal.details.age",
		"personal.details.contact.email",
		"personal.details.contact.phone.home",
		"personal.details.contact.phone.work",
		"professional.job",
		"professional.skills.0",
		"professional.projects.0.name",
		"professional.projects.0.details.status",
		"professional.projects.0.details.duration.months",
		"professional.projects.0.details.duration.days",
	}

	df, err := FlexibleToDataFrame(data, true, paths...)

	assert.NoError(t, err)
	assert.Equal(t, len(paths), df.Ncol())
	assert.Equal(t, 2, df.Nrow())

	// Check column names
	assert.Equal(t, paths, df.Names())

	// Check values
	assert.Equal(t, []string{"1", "2"}, df.Col("id").Records())
	assert.Equal(t, []string{"Alice", "Bob"}, df.Col("personal.name").Records())
	assert.Equal(t, []string{"30", "25"}, df.Col("personal.details.age").Records())
	assert.Equal(t, []string{"alice@example.com", "bob@example.com"}, df.Col("personal.details.contact.email").Records())
	assert.Equal(t, []string{"123-456-7890", "111-222-3333"}, df.Col("personal.details.contact.phone.home").Records())
	assert.Equal(t, []string{"098-765-4321", "444-555-6666"}, df.Col("personal.details.contact.phone.work").Records())
	assert.Equal(t, []string{"Engineer", "Designer"}, df.Col("professional.job").Records())
	assert.Equal(t, []string{"Go", "Photoshop"}, df.Col("professional.skills.0").Records())
	assert.Equal(t, []string{"Project A", "Project B"}, df.Col("professional.projects.0.name").Records())
	assert.Equal(t, []string{"Completed", "In Progress"}, df.Col("professional.projects.0.details.status").Records())
	assert.Equal(t, []string{"6", "3"}, df.Col("professional.projects.0.details.duration.months").Records())
	assert.Equal(t, []string{"15", "20"}, df.Col("professional.projects.0.details.duration.days").Records())

	// Check that the original data hasn't been modified
	assert.Equal(t, 1, data[0]["id"])
	assert.Equal(t, "Alice", data[0]["personal"].(map[string]interface{})["name"])
	assert.Equal(t, 30, data[0]["personal"].(map[string]interface{})["details"].(map[string]interface{})["age"])
	assert.Equal(t, "alice@example.com", data[0]["personal"].(map[string]interface{})["details"].(map[string]interface{})["contact"].(map[string]interface{})["email"])
	assert.Equal(t, "123-456-7890", data[0]["personal"].(map[string]interface{})["details"].(map[string]interface{})["contact"].(map[string]interface{})["phone"].(map[string]string)["home"])
	assert.Equal(t, "Engineer", data[0]["professional"].(map[string]interface{})["job"])
	assert.Equal(t, []string{"Go", "Python"}, data[0]["professional"].(map[string]interface{})["skills"])
	assert.Equal(t, "Project A", data[0]["professional"].(map[string]interface{})["projects"].([]map[string]interface{})[0]["name"])
	assert.Equal(t, "Completed", data[0]["professional"].(map[string]interface{})["projects"].([]map[string]interface{})[0]["details"].(map[string]interface{})["status"])
	assert.Equal(t, 6, data[0]["professional"].(map[string]interface{})["projects"].([]map[string]interface{})[0]["details"].(map[string]interface{})["duration"].(map[string]int)["months"])
}

func TestFlexibleToDataFrameNestedStructs(t *testing.T) {
	type Address struct {
		Street  string
		City    string
		Country struct {
			Name string
			Code string
		}
	}

	type Job struct {
		Title    string
		Company  string
		Duration struct {
			Years  int
			Months int
		}
	}

	type Person struct {
		ID         int
		Name       string
		Age        int
		Address    Address
		Occupation Job
		Skills     []string
	}

	data := []Person{
		{
			ID:   1,
			Name: "Alice",
			Age:  30,
			Address: Address{
				Street: "123 Main St",
				City:   "New York",
				Country: struct {
					Name string
					Code string
				}{
					Name: "USA",
					Code: "US",
				},
			},
			Occupation: Job{
				Title:   "Software Engineer",
				Company: "Tech Corp",
				Duration: struct {
					Years  int
					Months int
				}{
					Years:  5,
					Months: 3,
				},
			},
			Skills: []string{"Go", "Python", "JavaScript"},
		},
		{
			ID:   2,
			Name: "Bob",
			Age:  28,
			Address: Address{
				Street: "456 Elm St",
				City:   "London",
				Country: struct {
					Name string
					Code string
				}{
					Name: "United Kingdom",
					Code: "UK",
				},
			},
			Occupation: Job{
				Title:   "Data Scientist",
				Company: "Data Inc",
				Duration: struct {
					Years  int
					Months int
				}{
					Years:  3,
					Months: 6,
				},
			},
			Skills: []string{"Python", "R", "SQL"},
		},
	}

	paths := []string{
		"ID",
		"Name",
		"Age",
		"Address.Street",
		"Address.City",
		"Address.Country.Name",
		"Address.Country.Code",
		"Occupation.Title",
		"Occupation.Company",
		"Occupation.Duration.Years",
		"Occupation.Duration.Months",
		"Skills.0",
		"Skills.1",
		"Skills.2",
	}

	df, err := FlexibleToDataFrame(data, true, paths...)

	assert.NoError(t, err)
	assert.Equal(t, len(paths), df.Ncol())
	assert.Equal(t, 2, df.Nrow())

	// Check column names
	assert.Equal(t, paths, df.Names())

	// Check values
	assert.Equal(t, []string{"1", "2"}, df.Col("ID").Records())
	assert.Equal(t, []string{"Alice", "Bob"}, df.Col("Name").Records())
	assert.Equal(t, []string{"30", "28"}, df.Col("Age").Records())
	assert.Equal(t, []string{"123 Main St", "456 Elm St"}, df.Col("Address.Street").Records())
	assert.Equal(t, []string{"New York", "London"}, df.Col("Address.City").Records())
	assert.Equal(t, []string{"USA", "United Kingdom"}, df.Col("Address.Country.Name").Records())
	assert.Equal(t, []string{"US", "UK"}, df.Col("Address.Country.Code").Records())
	assert.Equal(t, []string{"Software Engineer", "Data Scientist"}, df.Col("Occupation.Title").Records())
	assert.Equal(t, []string{"Tech Corp", "Data Inc"}, df.Col("Occupation.Company").Records())
	assert.Equal(t, []string{"5", "3"}, df.Col("Occupation.Duration.Years").Records())
	assert.Equal(t, []string{"3", "6"}, df.Col("Occupation.Duration.Months").Records())
	assert.Equal(t, []string{"Go", "Python"}, df.Col("Skills.0").Records())
	assert.Equal(t, []string{"Python", "R"}, df.Col("Skills.1").Records())
	assert.Equal(t, []string{"JavaScript", "SQL"}, df.Col("Skills.2").Records())

	// Check that the original data hasn't been modified
	assert.Equal(t, 1, data[0].ID)
	assert.Equal(t, "Alice", data[0].Name)
	assert.Equal(t, 30, data[0].Age)
	assert.Equal(t, "123 Main St", data[0].Address.Street)
	assert.Equal(t, "New York", data[0].Address.City)
	assert.Equal(t, "USA", data[0].Address.Country.Name)
	assert.Equal(t, "US", data[0].Address.Country.Code)
	assert.Equal(t, "Software Engineer", data[0].Occupation.Title)
	assert.Equal(t, "Tech Corp", data[0].Occupation.Company)
	assert.Equal(t, 5, data[0].Occupation.Duration.Years)
	assert.Equal(t, 3, data[0].Occupation.Duration.Months)
	assert.Equal(t, []string{"Go", "Python", "JavaScript"}, data[0].Skills)
}
func TestMapToDataFrame(t *testing.T) {
	testCases := []struct {
		name       string
		data       interface{}
		topColumn  string
		strictMode bool
		paths      []string
		expected   dataframe.DataFrame
		expectErr  bool
	}{
		{
			name: "Simple map with single value",
			data: map[string]interface{}{
				"key1": []map[string]interface{}{
					{"name": "Alice", "age": 30},
				},
			},
			topColumn:  "category",
			strictMode: false,
			paths:      []string{"name", "age"},
			expected: dataframe.LoadRecords(
				[][]string{
					{"category", "name", "age"},
					{"key1", "Alice", "30"},
				},
			),
			expectErr: false,
		},
		{
			name: "Map with multiple values",
			data: map[string]interface{}{
				"key1": []map[string]interface{}{
					{"name": "Alice", "age": 30},
					{"name": "Bob", "age": 25},
				},
				"key2": []map[string]interface{}{
					{"name": "Charlie", "age": 35},
				},
			},
			topColumn:  "category",
			strictMode: false,
			paths:      []string{"name", "age"},
			expected: dataframe.LoadRecords(
				[][]string{
					{"category", "name", "age"},
					{"key1", "Alice", "30"},
					{"key1", "Bob", "25"},
					{"key2", "Charlie", "35"},
				},
			),
			expectErr: false,
		},
		{
			name: "Map with missing values",
			data: map[string]interface{}{
				"key1": []map[string]interface{}{
					{"name": "Alice"},
					{"age": 25},
				},
			},
			topColumn:  "category",
			strictMode: false,
			paths:      []string{"name", "age"},
			expected: dataframe.LoadRecords(
				[][]string{
					{"category", "name", "age"},
					{"key1", "Alice", "NaN"},
					{"key1", "NaN", "25"},
				},
			),
			expectErr: false,
		},
		{
			name:       "Invalid input (not a map)",
			data:       "not a map",
			topColumn:  "category",
			strictMode: false,
			paths:      []string{"name", "age"},
			expected:   dataframe.New(),
			expectErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := MapToDataFrame(tc.data, tc.topColumn, tc.strictMode, tc.paths...)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected.Names(), result.Names())
				assert.Equal(t, tc.expected.Records(), result.Records())
			}
		})
	}
}

func TestDeepSliceToDataFrame(t *testing.T) {
	tests := []struct {
		name           string
		data           interface{}
		topColumnPath  string
		slicePath      string
		strictMode     bool
		paths          []string
		expectedDF     dataframe.DataFrame
		expectedErrMsg string
	}{
		{
			name: "Valid deep slice",
			data: []map[string]interface{}{
				{
					"id": 1,
					"items": []map[string]interface{}{
						{"name": "Item1", "price": 10.5},
						{"name": "Item2", "price": 20.0},
					},
				},
				{
					"id": 2,
					"items": []map[string]interface{}{
						{"name": "Item3", "price": 15.0},
					},
				},
			},
			topColumnPath: "id",
			slicePath:     "items",
			strictMode:    true,
			paths:         []string{"name", "price"},
			expectedDF: dataframe.New(
				series.New([]int{1, 1, 2}, series.String, "id"),
				series.New([]string{"Item1", "Item2", "Item3"}, series.String, "name"),
				series.New([]float64{10.5, 20.0, 15.0}, series.Float, "price"),
			),
			expectedErrMsg: "",
		},
		{
			name: "Empty deep slice",
			data: []map[string]interface{}{
				{
					"id":    1,
					"items": []map[string]interface{}{},
				},
				{
					"id":    2,
					"items": []map[string]interface{}{},
				},
			},
			topColumnPath: "id",
			slicePath:     "items",
			strictMode:    true,
			paths:         []string{"name", "price"},
			expectedDF: dataframe.New(
				series.New([]int{}, series.String, "id"),
				series.New([]string{}, series.String, "name"),
				series.New([]float64{}, series.String, "price"),
			),
			expectedErrMsg: "",
		},
		{
			name:           "Invalid input - not a slice",
			data:           map[string]interface{}{"key": "value"},
			topColumnPath:  "id",
			slicePath:      "items",
			strictMode:     true,
			paths:          []string{"name", "price"},
			expectedDF:     dataframe.New(),
			expectedErrMsg: "input must be a slice",
		},
		{
			name: "Missing top column in strict mode",
			data: []map[string]interface{}{
				{
					"items": []map[string]interface{}{
						{"name": "Item1", "price": 10.5},
					},
				},
			},
			topColumnPath:  "id",
			slicePath:      "items",
			strictMode:     true,
			paths:          []string{"name", "price"},
			expectedDF:     dataframe.New(),
			expectedErrMsg: "error extracting top column value at index 0",
		},
		{
			name: "Missing deep slice in strict mode",
			data: []map[string]interface{}{
				{
					"id": 1,
				},
			},
			topColumnPath:  "id",
			slicePath:      "items",
			strictMode:     true,
			paths:          []string{"name", "price"},
			expectedDF:     dataframe.New(),
			expectedErrMsg: "error extracting deep slice at index 0",
		},

		{
			name: "Some empty deep slices",
			data: []map[string]interface{}{
				{
					"id": 1,
					"items": []map[string]interface{}{
						{"name": "Item1", "price": 10.5},
						{"name": "Item2", "price": 20.0},
					},
				},
				{
					"id":    2,
					"items": []map[string]interface{}{},
				},
				{
					"id": 3,
					"items": []map[string]interface{}{
						{"name": "Item3", "price": 15.0},
					},
				},
			},
			topColumnPath: "id",
			slicePath:     "items",
			strictMode:    false,
			paths:         []string{"name", "price"},
			expectedDF: dataframe.New(
				series.New([]int{1, 1, 3}, series.String, "id"),
				series.New([]string{"Item1", "Item2", "Item3"}, series.String, "name"),
				series.New([]float64{10.5, 20.0, 15.0}, series.Float, "price"),
			),
			expectedErrMsg: "",
		},
		{
			name: "All empty deep slices",
			data: []map[string]interface{}{
				{
					"id":    1,
					"items": []map[string]interface{}{},
				},
				{
					"id":    2,
					"items": []map[string]interface{}{},
				},
				{
					"id":    3,
					"items": []map[string]interface{}{},
				},
			},
			topColumnPath: "id",
			slicePath:     "items",
			strictMode:    false,
			paths:         []string{"name", "price"},
			expectedDF: dataframe.New(
				series.New([]int{}, series.String, "id"),
				series.New([]string{}, series.String, "name"),
				series.New([]float64{}, series.String, "price"),
			),
			expectedErrMsg: "",
		},
		{
			name: "Mixed deep slices with some empty and some missing",
			data: []map[string]interface{}{
				{
					"id": 1,
					"items": []map[string]interface{}{
						{"name": "Item1", "price": 10.5},
					},
				},
				{
					"id":    2,
					"items": []map[string]interface{}{},
				},
				{
					"id": 3,
				},
			},
			topColumnPath: "id",
			slicePath:     "items",
			strictMode:    false,
			paths:         []string{"name", "price"},
			expectedDF: dataframe.New(
				series.New([]int{1}, series.String, "id"),
				series.New([]string{"Item1"}, series.String, "name"),
				series.New([]float64{10.5}, series.Float, "price"),
			),
			expectedErrMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df, err := DeepSliceToDataFrame(tt.data, tt.topColumnPath, tt.slicePath, tt.strictMode, tt.paths...)

			if tt.expectedErrMsg != "" {
				if err == nil {
					t.Errorf("Expected error containing '%s', but got nil", tt.expectedErrMsg)
				} else if !strings.Contains(err.Error(), tt.expectedErrMsg) {
					t.Errorf("Expected error containing '%s', but got '%s'", tt.expectedErrMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if !reflect.DeepEqual(df.Names(), tt.expectedDF.Names()) {
					t.Errorf("Column names mismatch.\nExpected: %v\nGot: %v", tt.expectedDF.Names(), df.Names())
				}

				if !reflect.DeepEqual(df.Types(), tt.expectedDF.Types()) {
					t.Errorf("Column types mismatch.\nExpected: %v\nGot: %v", tt.expectedDF.Types(), df.Types())
				}

				if df.Nrow() != tt.expectedDF.Nrow() {
					t.Errorf("Row count mismatch.\nExpected: %d\nGot: %d", tt.expectedDF.Nrow(), df.Nrow())
				}

				for _, colName := range df.Names() {
					expectedCol := tt.expectedDF.Col(colName)
					actualCol := df.Col(colName)

					if !reflect.DeepEqual(expectedCol.Records(), actualCol.Records()) {
						t.Errorf("Column %s data mismatch.\nExpected: %v\nGot: %v", colName, expectedCol.Records(), actualCol.Records())
					}
				}
			}
		})
	}
}

func TestDataframeToStruct(t *testing.T) {
	type TestStruct struct {
		Name       string   `json:"name" required:"true"`
		Age        int      `json:"age" required:"true"`
		Score      float64  `json:"score"`
		IsActive   bool     `json:"is_active"`
		Tags       []string `json:"tags"`
		ExtraField string   `json:"extra_field"`
	}

	tests := []struct {
		name     string
		df       dataframe.DataFrame
		expected []TestStruct
		wantErr  bool
	}{
		{
			name: "Successful conversion with all required fields",
			df: dataframe.New(
				series.New([]string{"Alice", "Bob"}, series.String, "name"),
				series.New([]int{25, 30}, series.Int, "age"),
				series.New([]float64{95.5, 88.0}, series.Float, "score"),
				series.New([]bool{true, false}, series.Bool, "is_active"),
			),
			expected: []TestStruct{
				{Name: "Alice", Age: 25, Score: 95.5, IsActive: true},
				{Name: "Bob", Age: 30, Score: 88.0, IsActive: false},
			},
			wantErr: false,
		},
		{
			name: "Error due to missing required field",
			df: dataframe.New(
				series.New([]string{"Charlie", "David"}, series.String, "name"),
				series.New([]float64{90, 85}, series.Float, "score"),
			),
			expected: nil,
			wantErr:  true,
		},
		{
			name: "Successful conversion with only required fields",
			df: dataframe.New(
				series.New([]string{"Eve", "Frank"}, series.String, "name"),
				series.New([]int{28, 32}, series.Int, "age"),
			),
			expected: []TestStruct{
				{Name: "Eve", Age: 28},
				{Name: "Frank", Age: 32},
			},
			wantErr: false,
		},
		{
			name: "Successful conversion with extra fields in DataFrame",
			df: dataframe.New(
				series.New([]string{"Grace", "Henry"}, series.String, "name"),
				series.New([]int{29, 33}, series.Int, "age"),
				series.New([]float64{93.5, 86.5}, series.Float, "score"),
				series.New([]string{"extra1", "extra2"}, series.String, "extra_column"),
			),
			expected: []TestStruct{
				{Name: "Grace", Age: 29, Score: 93.5},
				{Name: "Henry", Age: 33, Score: 86.5},
			},
			wantErr: false,
		},
		// {
		// 	name: "Error due to wrong type for required field",
		// 	df: dataframe.New(
		// 		series.New([]string{"Mia", "Noah"}, series.String, "name"),
		// 		series.New([]string{"27", "31"}, series.String, "age"),
		// 	),
		// 	expected: nil,
		// 	wantErr:  true,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DataframeToStruct[TestStruct](tt.df)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDeepSliceToSlice(t *testing.T) {
	type TestStruct struct {
		X int
		Y string
	}

	tests := []struct {
		name       string
		data       interface{}
		element    TestStruct
		slicePath  string
		strictMode bool
		paths      []string
		expected   []TestStruct
		expectErr  bool
	}{
		{
			name: "Valid nested slice",
			data: []map[string]interface{}{
				{
					"deepSlice": []map[string]interface{}{
						{"X": 1, "Y": "a"},
						{"X": 2, "Y": "b"},
					},
				},
				{
					"deepSlice": []map[string]interface{}{
						{"X": 3, "Y": "c"},
						{"X": 4, "Y": "d"},
					},
				},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: true,
			paths:      []string{"X", "Y"},
			expected: []TestStruct{
				{X: 1, Y: "a"},
				{X: 2, Y: "b"},
				{X: 3, Y: "c"},
				{X: 4, Y: "d"},
			},
			expectErr: false,
		},
		{
			name: "Empty nested slice",
			data: []map[string]interface{}{
				{"deepSlice": []map[string]interface{}{}},
				{"deepSlice": []map[string]interface{}{}},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: true,
			paths:      []string{"X", "Y"},
			expected:   []TestStruct{},
			expectErr:  false,
		},
		{
			name:       "Invalid input - not a slice",
			data:       map[string]interface{}{"key": "value"},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: true,
			paths:      []string{"X", "Y"},
			expected:   nil,
			expectErr:  true,
		},
		{
			name: "Missing deep slice in strict mode",
			data: []map[string]interface{}{
				{"otherKey": "value"},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: true,
			paths:      []string{"X", "Y"},
			expected:   nil,
			expectErr:  true,
		},
		{
			name: "Missing deep slice in non-strict mode",
			data: []map[string]interface{}{
				{"otherKey": "value"},
				{
					"deepSlice": []map[string]interface{}{
						{"X": 1, "Y": "a"},
					},
				},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: false,
			paths:      []string{"X", "Y"},
			expected: []TestStruct{
				{X: 1, Y: "a"},
			},
			expectErr: false,
		},
		{
			name: "Missing field in strict mode",
			data: []map[string]interface{}{
				{
					"deepSlice": []map[string]interface{}{
						{"X": 1},
						{"Y": "b"},
					},
				},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: true,
			paths:      []string{"X", "Y"},
			expected:   nil,
			expectErr:  true,
		},
		{
			name: "Missing field in non-strict mode",
			data: []map[string]interface{}{
				{
					"deepSlice": []map[string]interface{}{
						{"X": 1},
						{"Y": "b"},
					},
				},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: false,
			paths:      []string{"X", "Y"},
			expected: []TestStruct{
				{X: 1, Y: ""},
				{X: 0, Y: "b"},
			},
			expectErr: false,
		},
		{
			name: "Empty paths - return entire slice",
			data: []map[string]interface{}{
				{
					"deepSlice": []TestStruct{
						{X: 1, Y: "a"},
						{X: 2, Y: "b"},
					},
				},
				{
					"deepSlice": []TestStruct{
						{X: 3, Y: "c"},
					},
				},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: true,
			paths:      []string{}, // Empty paths
			expected: []TestStruct{
				{X: 1, Y: "a"},
				{X: 2, Y: "b"},
				{X: 3, Y: "c"},
			},
			expectErr: false,
		},
		{
			name: "Empty paths with mixed types - should fail",
			data: []map[string]interface{}{
				{
					"deepSlice": []interface{}{
						TestStruct{X: 1, Y: "a"},
						map[string]interface{}{"X": 2, "Y": "b"},
					},
				},
			},
			element:    TestStruct{},
			slicePath:  "deepSlice",
			strictMode: true,
			paths:      []string{}, // Empty paths
			expected:   nil,
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DeepSliceToSlice(tt.data, tt.element, tt.slicePath, tt.strictMode, tt.paths...)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
func TestDeepCopy(t *testing.T) {
	t.Run("Basic Types", func(t *testing.T) {
		testCases := []struct {
			name string
			src  interface{}
		}{
			{"Int", 42},
			{"Int8", int8(8)},
			{"Int16", int16(16)},
			{"Int32", int32(32)},
			{"Int64", int64(64)},
			{"Uint", uint(42)},
			{"Uint8", uint8(8)},
			{"Uint16", uint16(16)},
			{"Uint32", uint32(32)},
			{"Uint64", uint64(64)},
			{"Float32", float32(3.14)},
			{"Float64", 3.14},
			{"Complex64", complex64(1 + 2i)},
			{"Complex128", 1 + 2i},
			{"String", "hello"},
			{"Bool", true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dst := reflect.New(reflect.TypeOf(tc.src)).Interface()
				err := DeepCopy(dst, tc.src)
				if err != nil {
					t.Fatalf("DeepCopy failed: %v", err)
				}
				if reflect.ValueOf(dst).Elem().Interface() != tc.src {
					t.Errorf("DeepCopy result mismatch. Got %v, want %v", reflect.ValueOf(dst).Elem().Interface(), tc.src)
				}
			})
		}
	})
	t.Run("Pointers", func(t *testing.T) {
		x := 42
		src := &x
		var dst int
		err := DeepCopy(&dst, src)
		if err != nil {
			t.Fatalf("DeepCopy failed: %v", err)
		}
		if dst != *src {
			t.Errorf("DeepCopy result mismatch. Got %v, want %v", dst, *src)
		}
	})

	t.Run("Structs", func(t *testing.T) {
		type Person struct {
			Name string
			Age  int
		}
		src := Person{Name: "Alice", Age: 30}
		var dst Person
		err := DeepCopy(&dst, &src) // 注意这里改为传递指针
		if err != nil {
			t.Fatalf("DeepCopy failed: %v", err)
		}
		if !reflect.DeepEqual(dst, src) {
			t.Errorf("DeepCopy result mismatch. Got %v, want %v", dst, src)
		}
	})

	t.Run("Slices", func(t *testing.T) {
		testCases := []struct {
			name string
			src  interface{}
		}{
			{"IntSlice", []int{1, 2, 3, 4, 5}},
			{"StringSlice", []string{"a", "b", "c"}},
			{"FloatSlice", []float64{1.1, 2.2, 3.3}},
			{"EmptySlice", []int{}},
			{"NilSlice", []int(nil)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dst := reflect.New(reflect.TypeOf(tc.src)).Interface()
				err := DeepCopy(dst, tc.src)
				if err != nil {
					t.Fatalf("DeepCopy failed: %v", err)
				}
				if !reflect.DeepEqual(reflect.ValueOf(dst).Elem().Interface(), tc.src) {
					t.Errorf("DeepCopy result mismatch. Got %v, want %v", reflect.ValueOf(dst).Elem().Interface(), tc.src)
				}
			})
		}
	})

	t.Run("Arrays", func(t *testing.T) {
		testCases := []struct {
			name string
			src  interface{}
		}{
			{"IntArray", [5]int{1, 2, 3, 4, 5}},
			{"StringArray", [3]string{"a", "b", "c"}},
			{"FloatArray", [2]float64{1.1, 2.2}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dst := reflect.New(reflect.TypeOf(tc.src)).Interface()
				err := DeepCopy(dst, tc.src)
				if err != nil {
					t.Fatalf("DeepCopy failed: %v", err)
				}
				if !reflect.DeepEqual(reflect.ValueOf(dst).Elem().Interface(), tc.src) {
					t.Errorf("DeepCopy result mismatch. Got %v, want %v", reflect.ValueOf(dst).Elem().Interface(), tc.src)
				}
			})
		}
	})

	t.Run("Maps", func(t *testing.T) {
		testCases := []struct {
			name string
			src  interface{}
		}{
			{"StringIntMap", map[string]int{"one": 1, "two": 2, "three": 3}},
			{"IntStringMap", map[int]string{1: "one", 2: "two", 3: "three"}},
			{"EmptyMap", map[string]int{}},
			{"NilMap", map[string]int(nil)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dst := reflect.New(reflect.TypeOf(tc.src)).Interface()
				err := DeepCopy(dst, tc.src)
				if err != nil {
					t.Fatalf("DeepCopy failed: %v", err)
				}
				if !reflect.DeepEqual(reflect.ValueOf(dst).Elem().Interface(), tc.src) {
					t.Errorf("DeepCopy result mismatch. Got %v, want %v", reflect.ValueOf(dst).Elem().Interface(), tc.src)
				}
			})
		}
	})

	t.Run("Nested Structures", func(t *testing.T) {
		type Address struct {
			Street string
			City   string
		}

		type Person struct {
			Name    string
			Age     int
			Address Address
			Hobbies []string
			Scores  map[string]int
		}

		src := Person{
			Name: "Bob",
			Age:  25,
			Address: Address{
				Street: "123 Main St",
				City:   "Anytown",
			},
			Hobbies: []string{"reading", "swimming"},
			Scores:  map[string]int{"math": 95, "science": 88},
		}
		var dst Person
		err := DeepCopy(&dst, src)
		if err != nil {
			t.Fatalf("DeepCopy failed: %v", err)
		}
		if !reflect.DeepEqual(dst, src) {
			t.Errorf("DeepCopy result mismatch. Got %v, want %v", dst, src)
		}
	})

	t.Run("Circular References", func(t *testing.T) {
		type Node struct {
			Value int
			Next  *Node
		}
		src := &Node{Value: 1}
		src.Next = &Node{Value: 2}
		src.Next.Next = src // Create a circular reference

		var dst Node
		err := DeepCopy(&dst, src)
		if err != nil {
			t.Fatalf("DeepCopy failed: %v", err)
		}
		if dst.Value != src.Value || dst.Next.Value != src.Next.Value {
			t.Errorf("DeepCopy result mismatch. Got %v -> %v, want %v -> %v",
				dst.Value, dst.Next.Value, src.Value, src.Next.Value)
		}
		if dst.Next.Next == &dst {
			t.Errorf("DeepCopy did not break circular reference")
		}
	})

	t.Run("Interfaces", func(t *testing.T) {
		testCases := []struct {
			name string
			src  interface{}
		}{
			{"StringInterface", interface{}("test string")},
			{"IntInterface", interface{}(42)},
			{"StructInterface", interface{}(struct{ X int }{X: 10})},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dst := reflect.New(reflect.TypeOf(tc.src)).Interface()
				err := DeepCopy(dst, tc.src)
				if err != nil {
					t.Fatalf("DeepCopy failed: %v", err)
				}
				if !reflect.DeepEqual(reflect.ValueOf(dst).Elem().Interface(), tc.src) {
					t.Errorf("DeepCopy result mismatch. Got %v, want %v", reflect.ValueOf(dst).Elem().Interface(), tc.src)
				}
			})
		}
	})

	t.Run("Type Mismatch", func(t *testing.T) {
		testCases := []struct {
			name string
			src  interface{}
			dst  interface{}
		}{
			{"IntToString", 42, ""},
			{"StringToInt", "42", 0},
			{"FloatToInt", 3.14, 0},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := DeepCopy(reflect.ValueOf(&tc.dst).Elem(), reflect.ValueOf(tc.src))
				if err == nil {
					t.Fatalf("deepCopy should have failed due to type mismatch")
				}
			})
		}
	})

	t.Run("Special Values", func(t *testing.T) {
		testCases := []struct {
			name string
			src  interface{}
		}{
			{"MaxInt", int(^uint(0) >> 1)},
			{"MinInt", -int(^uint(0)>>1) - 1},
			{"NaN", math.NaN()},
			{"PosInf", math.Inf(1)},
			{"NegInf", math.Inf(-1)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dst := reflect.New(reflect.TypeOf(tc.src)).Interface()
				err := DeepCopy(dst, tc.src)
				if err != nil {
					t.Fatalf("DeepCopy failed: %v", err)
				}
				if tc.name == "NaN" {
					if !math.IsNaN(reflect.ValueOf(dst).Elem().Float()) {
						t.Errorf("Expected NaN, got %v", reflect.ValueOf(dst).Elem().Interface())
					}
				} else if !reflect.DeepEqual(reflect.ValueOf(dst).Elem().Interface(), tc.src) {
					t.Errorf("DeepCopy result mismatch. Got %v, want %v", reflect.ValueOf(dst).Elem().Interface(), tc.src)
				}
			})
		}
	})
}
