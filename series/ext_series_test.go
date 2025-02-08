package series

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValuesIterator(t *testing.T) {
	t.Run("Basic iteration", func(t *testing.T) {
		s := New([]int{1, 2, 3, 4, 5}, Int, "test")
		iter := s.ValuesIterator()
		expected := []int{1, 2, 3, 4, 5}
		i := 0
		for index, value, ok := iter(); ok; index, value, ok = iter() {
			assert.Equal(t, i, index)
			assert.Equal(t, expected[i], value)
			i++
		}
		assert.Equal(t, len(expected), i)
	})

	t.Run("Reverse iteration", func(t *testing.T) {
		s := New([]int{1, 2, 3, 4, 5}, Int, "test")
		iter := s.ValuesIterator(WithReverse(true))
		expected := []int{5, 4, 3, 2, 1}
		i := 0
		for index, value, ok := iter(); ok; index, value, ok = iter() {
			assert.Equal(t, 4-i, index)
			assert.Equal(t, expected[i], value)
			i++
		}
		assert.Equal(t, len(expected), i)
	})

	t.Run("Step iteration", func(t *testing.T) {
		s := New([]int{1, 2, 3, 4, 5}, Int, "test")
		iter := s.ValuesIterator(WithStep(2))
		expected := []int{1, 3, 5}
		i := 0
		for index, value, ok := iter(); ok; index, value, ok = iter() {
			assert.Equal(t, i*2, index)
			assert.Equal(t, expected[i], value)
			i++
		}
		assert.Equal(t, len(expected), i)
	})

	// t.Run("Skip NaN", func(t *testing.T) {
	//     s := New([]float64{1.0, NaN, 3.0, NaN, 5.0}, Float, "test")
	//     iter := s.ValuesIterator(ValuesOptions{SkipNaN: true})
	//     expected := []float64{1.0, 3.0, 5.0}
	//     i := 0
	//     for _, value, ok := iter(); ok; _, value, ok = iter() {
	//         assert.Equal(t, expected[i], value)
	//         i++
	//     }
	//     assert.Equal(t, len(expected), i)
	// })

	t.Run("Only unique", func(t *testing.T) {
		s := New([]int{1, 2, 2, 3, 3, 3, 4}, Int, "test")
		iter := s.ValuesIterator(WithOnlyUnique(true))
		expected := []int{1, 2, 3, 4}
		i := 0
		for _, value, ok := iter(); ok; _, value, ok = iter() {
			assert.Equal(t, expected[i], value)
			i++
		}
		assert.Equal(t, len(expected), i)
	})

	// t.Run("Combination of options", func(t *testing.T) {
	//     s := New([]float64{1.0, 2.0, NaN, 3.0, 2.0, NaN, 4.0}, Float, "test")
	//     iter := s.ValuesIterator(ValuesOptions{
	//         Reverse:    true,
	//         Step:       2,
	//         SkipNaN:    true,
	//         OnlyUnique: true,
	//     })
	//     expected := []float64{4.0, 2.0, 1.0}
	//     i := 0
	//     for _, value, ok := iter(); ok; _, value, ok = iter() {
	//         assert.Equal(t, expected[i], value)
	//         i++
	//     }
	//     assert.Equal(t, len(expected), i)
	// })

	t.Run("Empty series", func(t *testing.T) {
		s := New([]int{}, Int, "test")
		iter := s.ValuesIterator()
		_, _, ok := iter()
		assert.False(t, ok)
	})
}

func TestNewFromIterator(t *testing.T) {
	t.Run("Float Series", func(t *testing.T) {
		values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
		it := func() iterator {
			i := 0
			return func() (int, interface{}, bool) {
				if i >= len(values) {
					return -1, nil, false
				}
				v := values[i]
				i++
				return i - 1, v, true
			}
		}()

		s := NewFromIterator(it, "FloatSeries")
		assert.Equal(t, "FloatSeries", s.Name)
		assert.Equal(t, Float, s.Type())
		assert.Equal(t, 5, s.Len())
		assert.Equal(t, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, s.Float())
	})

	t.Run("String Series", func(t *testing.T) {
		values := []string{"a", "b", "c", "d", "e"}
		it := func() iterator {
			i := 0
			return func() (int, interface{}, bool) {
				if i >= len(values) {
					return -1, nil, false
				}
				v := values[i]
				i++
				return i - 1, v, true
			}
		}()

		s := NewFromIterator(it, "StringSeries")
		assert.Equal(t, "StringSeries", s.Name)
		assert.Equal(t, String, s.Type())
		assert.Equal(t, 5, s.Len())
		assert.Equal(t, values, s.Records())
	})

	t.Run("Bool Series", func(t *testing.T) {
		values := []bool{true, false, true, false, true}
		it := func() iterator {
			i := 0
			return func() (int, interface{}, bool) {
				if i >= len(values) {
					return -1, nil, false
				}
				v := values[i]
				i++
				return i - 1, v, true
			}
		}()

		s := NewFromIterator(it, "BoolSeries")
		assert.Equal(t, "BoolSeries", s.Name)
		assert.Equal(t, Bool, s.Type())
		assert.Equal(t, 5, s.Len())
		bools, err := s.Bool()
		assert.NoError(t, err)
		assert.Equal(t, values, bools)
	})

	// t.Run("Empty Iterator", func(t *testing.T) {
	// 	it := func() iterator {
	// 		return func() (int, interface{}, bool) {
	// 			return -1, nil, false
	// 		}
	// 	}()

	// 	s := NewFromIterator(it, "EmptySeries")
	// 	assert.Equal(t, "EmptySeries", s.Name)
	// 	assert.Equal(t, 0, s.Len())
	// })

	// t.Run("Mixed Types (defaults to String)", func(t *testing.T) {
	// 	values := []interface{}{1, "two", 3.0, true, "five"}
	// 	it := func() iterator {
	// 		i := 0
	// 		return func() (int, interface{}, bool) {
	// 			if i >= len(values) {
	// 				return -1, nil, false
	// 			}
	// 			v := values[i]
	// 			i++
	// 			return i - 1, v, true
	// 		}
	// 	}()

	// 	s := NewFromIterator(it, "MixedSeries")
	// 	assert.Equal(t, "MixedSeries", s.Name)
	// 	assert.Equal(t, String, s.Type())
	// 	assert.Equal(t, 5, s.Len())
	// 	assert.Equal(t, []string{"1", "two", "3", "true", "five"}, s.Records())
	// })
}
