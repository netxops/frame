package series

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeries_Add(t *testing.T) {
	tests := []struct {
		name     string
		s        Series
		value    interface{}
		expected Series
	}{
		{
			name:     "Add int to Float Series",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    5,
			expected: New([]float64{6.0, 7.0, 8.0}, Float, "test_add_int"),
		},
		{
			name:     "Add float64 to Float Series",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    2.5,
			expected: New([]float64{3.5, 4.5, 5.5}, Float, "test_add_float64"),
		},
		{
			name:     "Add Series to Float Series",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    New([]float64{0.5, 1.5, 2.5}, Float, "other"),
			expected: New([]float64{1.5, 3.5, 5.5}, Float, "test_add_other"),
		},
		{
			name:     "Add int to Int Series",
			s:        New([]int{1, 2, 3}, Int, "test"),
			value:    5,
			expected: New([]int{6, 7, 8}, Int, "test_add_int"),
		},
		{
			name:     "Add Int Series to Int Series",
			s:        New([]int{1, 2, 3}, Int, "test"),
			value:    New([]int{4, 5, 6}, Int, "other"),
			expected: New([]int{5, 7, 9}, Int, "test_add_other"),
		},
		{
			name:     "Add to empty Series",
			s:        New([]float64{}, Float, "test"),
			value:    5,
			expected: New([]float64{}, Float, "test_add_int"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.s.Add(tt.value, "")
			assert.Equal(t, tt.expected.Type(), result.Type())
			assert.Equal(t, tt.expected.Name, result.Name)
			assert.Equal(t, tt.expected.Len(), result.Len())

			for i := 0; i < result.Len(); i++ {
				assert.Equal(t, tt.expected.Val(i), result.Val(i))
			}
		})
	}
}

func TestSeries_Add_Errors(t *testing.T) {
	s := New([]float64{1.0, 2.0, 3.0}, Float, "test")

	t.Run("Add unsupported type", func(t *testing.T) {
		result := s.Add("invalid", "")
		assert.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "unsupported type for arithmetic operation")
	})

	t.Run("Add Series with different length", func(t *testing.T) {
		other := New([]float64{1.0, 2.0}, Float, "other")
		result := s.Add(other, "")
		assert.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "cannot perform operation on series of different lengths")
	})
}

func TestSeries_Add_NaN(t *testing.T) {
	s := New([]float64{1.0, math.NaN(), 3.0}, Float, "test")
	result := s.Add(2.0, "")

	assert.Equal(t, Float, result.Type())
	assert.Equal(t, 3, result.Len())
	// assert.InDelta(t, 3.0, result.Val(0).(float64), 1e-7)
	// assert.True(t, math.IsNaN(result.Val(1).(float64)))
	// assert.InDelta(t, 5.0, result.Val(2).(float64), 1e-7)
}

func TestSeries_Add_IntOverflow(t *testing.T) {
	s := New([]int{math.MaxInt64, 1}, Int, "test")
	result := s.Add(1, "")

	assert.Equal(t, Int, result.Type())
	assert.Equal(t, 2, result.Len())
	assert.Equal(t, math.MinInt64, result.Val(0))
	assert.Equal(t, 2, result.Val(1))
}

func TestSeries_Arithmetic(t *testing.T) {
	tests := []struct {
		name     string
		s        Series
		value    interface{}
		op       string
		expected Series
	}{
		// Add tests
		{
			name:     "Add int to Float Series",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    5,
			op:       "add",
			expected: New([]float64{6.0, 7.0, 8.0}, Float, "test_add_int"),
		},
		{
			name:     "Add float64 to Float Series",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    2.5,
			op:       "add",
			expected: New([]float64{3.5, 4.5, 5.5}, Float, "test_add_float64"),
		},
		{
			name:     "Add Series to Float Series",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    New([]float64{0.5, 1.5, 2.5}, Float, "other"),
			op:       "add",
			expected: New([]float64{1.5, 3.5, 5.5}, Float, "test_add_other"),
		},
		{
			name:     "Add int to Int Series",
			s:        New([]int{1, 2, 3}, Int, "test"),
			value:    5,
			op:       "add",
			expected: New([]int{6, 7, 8}, Int, "test_add_int"),
		},
		// Subtract tests
		{
			name:     "Subtract int from Float Series",
			s:        New([]float64{6.0, 7.0, 8.0}, Float, "test"),
			value:    5,
			op:       "sub",
			expected: New([]float64{1.0, 2.0, 3.0}, Float, "test_sub_int"),
		},
		{
			name:     "Subtract float64 from Float Series",
			s:        New([]float64{3.5, 4.5, 5.5}, Float, "test"),
			value:    2.5,
			op:       "sub",
			expected: New([]float64{1.0, 2.0, 3.0}, Float, "test_sub_float64"),
		},
		{
			name:     "Subtract Series from Float Series",
			s:        New([]float64{1.5, 3.5, 5.5}, Float, "test"),
			value:    New([]float64{0.5, 1.5, 2.5}, Float, "other"),
			op:       "sub",
			expected: New([]float64{1.0, 2.0, 3.0}, Float, "test_sub_other"),
		},
		{
			name:     "Subtract int from Int Series",
			s:        New([]int{6, 7, 8}, Int, "test"),
			value:    5,
			op:       "sub",
			expected: New([]int{1, 2, 3}, Int, "test_sub_int"),
		},
		// Multiply tests
		{
			name:     "Multiply Float Series by int",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    2,
			op:       "mul",
			expected: New([]float64{2.0, 4.0, 6.0}, Float, "test_mul_int"),
		},
		{
			name:     "Multiply Float Series by float64",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    2.5,
			op:       "mul",
			expected: New([]float64{2.5, 5.0, 7.5}, Float, "test_mul_float64"),
		},
		{
			name:     "Multiply Float Series by Series",
			s:        New([]float64{1.0, 2.0, 3.0}, Float, "test"),
			value:    New([]float64{0.5, 1.5, 2.5}, Float, "other"),
			op:       "mul",
			expected: New([]float64{0.5, 3.0, 7.5}, Float, "test_mul_other"),
		},
		{
			name:     "Multiply Int Series by int",
			s:        New([]int{1, 2, 3}, Int, "test"),
			value:    2,
			op:       "mul",
			expected: New([]int{2, 4, 6}, Int, "test_mul_int"),
		},
		// Divide tests
		{
			name:     "Divide Float Series by int",
			s:        New([]float64{2.0, 4.0, 6.0}, Float, "test"),
			value:    2,
			op:       "div",
			expected: New([]float64{1.0, 2.0, 3.0}, Float, "test_div_int"),
		},
		{
			name:     "Divide Float Series by float64",
			s:        New([]float64{2.5, 5.0, 7.5}, Float, "test"),
			value:    2.5,
			op:       "div",
			expected: New([]float64{1.0, 2.0, 3.0}, Float, "test_div_float64"),
		},
		{
			name:     "Divide Float Series by Series",
			s:        New([]float64{0.5, 3.0, 7.5}, Float, "test"),
			value:    New([]float64{0.5, 1.5, 2.5}, Float, "other"),
			op:       "div",
			expected: New([]float64{1.0, 2.0, 3.0}, Float, "test_div_other"),
		},
		{
			name:     "Divide Int Series by int",
			s:        New([]int{2, 4, 6}, Int, "test"),
			value:    2,
			op:       "div",
			expected: New([]int{1, 2, 3}, Int, "test_div_int"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result Series
			switch tt.op {
			case "add":
				result = tt.s.Add(tt.value, "")
			case "sub":
				result = tt.s.Sub(tt.value, "")
			case "mul":
				result = tt.s.Mul(tt.value, "")
			case "div":
				result = tt.s.Div(tt.value, "")
			}

			assert.Equal(t, tt.expected.Type(), result.Type())
			assert.Equal(t, tt.expected.Name, result.Name)
			assert.Equal(t, tt.expected.Len(), result.Len())

			for i := 0; i < result.Len(); i++ {
				if tt.expected.Type() == Float {
					assert.InDelta(t, tt.expected.Val(i).(float64), result.Val(i).(float64), 1e-7)
				} else {
					assert.Equal(t, tt.expected.Val(i), result.Val(i))
				}
			}
		})
	}
}

func TestSeries_Arithmetic_Errors(t *testing.T) {
	s := New([]float64{1.0, 2.0, 3.0}, Float, "test")

	operations := []string{"Add", "Sub", "Mul", "Div"}

	for _, op := range operations {
		t.Run(op+" unsupported type", func(t *testing.T) {
			var result Series
			switch op {
			case "Add":
				result = s.Add("invalid", "")
			case "Sub":
				result = s.Sub("invalid", "")
			case "Mul":
				result = s.Mul("invalid", "")
			case "Div":
				result = s.Div("invalid", "")
			}
			assert.Error(t, result.Err)
			assert.Contains(t, result.Err.Error(), "unsupported type for arithmetic operation")
		})

		t.Run(op+" Series with different length", func(t *testing.T) {
			other := New([]float64{1.0, 2.0}, Float, "other")
			var result Series
			switch op {
			case "Add":
				result = s.Add(other, "")
			case "Sub":
				result = s.Sub(other, "")
			case "Mul":
				result = s.Mul(other, "")
			case "Div":
				result = s.Div(other, "")
			}
			assert.Error(t, result.Err)
			assert.Contains(t, result.Err.Error(), "cannot perform operation on series of different lengths")
		})
	}
}

func TestSeries_Arithmetic_SpecialCases(t *testing.T) {
	t.Run("Division by zero", func(t *testing.T) {
		s := New([]float64{1.0, 2.0, 3.0}, Float, "test")
		result := s.Div(0, "")
		assert.Error(t, result.Err)
		assert.Equal(t, Float, result.Type())
		assert.Equal(t, 3, result.Len())
	})

	t.Run("NaN handling", func(t *testing.T) {
		s := New([]float64{1.0, math.NaN(), 3.0}, Float, "test")
		result := s.Add(2.0, "")
		assert.Error(t, result.Err)
		assert.Equal(t, Float, result.Type())
		assert.Equal(t, 3, result.Len())
	})

	t.Run("Integer overflow", func(t *testing.T) {
		s := New([]int{math.MaxInt64, 1}, Int, "test")
		result := s.Add(1, "")
		assert.NoError(t, result.Err)
		assert.Equal(t, Int, result.Type())
		assert.Equal(t, 2, result.Len())
	})
}
