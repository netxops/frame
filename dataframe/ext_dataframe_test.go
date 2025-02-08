package dataframe

import (
	"testing"

	"github.com/netxops/frame/series"
	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	// 创建测试用的DataFrame
	df := New(
		series.New([]int{1, 2, 3, 4, 5}, series.Int, "A"),
		series.New([]int{5, 4, 3, 2, 1}, series.Int, "B"),
		series.New([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, series.Float, "C"),
		series.New([]string{"a", "b", "c", "d", "e"}, series.String, "D"),
		series.New([]int{2, 2, 2, 2, 2}, series.Int, "F"),
	)

	tests := []struct {
		name       string
		newColName string
		columns    []string
		expected   series.Series
	}{
		{
			name:       "Single int column",
			columns:    []string{"A"},
			newColName: "A",
			expected:   series.New([]int{1, 2, 3, 4, 5}, series.Int, "A"),
		},
		{
			name:       "Two int columns",
			columns:    []string{"A", "B"},
			newColName: "A",
			expected:   series.New([]int{1, 2, 3, 2, 1}, series.Int, "A"),
		},

		{
			name:       "Three int columns",
			columns:    []string{"A", "B", "F"},
			newColName: "A",
			expected:   series.New([]int{1, 2, 2, 2, 1}, series.Int, "A"),
		},
		{
			name:       "Int and float columns",
			columns:    []string{"A", "C"},
			newColName: "A",
			expected:   series.New([]int{1, 2, 3, 4, 5}, series.Int, "A"),
		},
		{
			name:       "Non-existent column",
			columns:    []string{"E"},
			newColName: "E",
			expected:   series.New(nil, series.Float, ""),
		},
		{
			name:       "String column (should be ignored)",
			columns:    []string{"D"},
			newColName: "E",
			expected:   series.New(nil, series.Float, ""),
		},
		{
			name:       "Mix of valid and invalid columns",
			columns:    []string{"A", "D", "B"},
			newColName: "A",
			expected:   series.New([]int{1, 2, 3, 2, 1}, series.Int, "A"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MinInColumns(df, tt.newColName, tt.columns...)
			assert.Equal(t, tt.expected.Records(), result.Records(), "Min function returned unexpected result")
		})
	}
}

func TestMax(t *testing.T) {
	// 创建测试用的DataFrame
	df := New(
		series.New([]int{1, 2, 3, 4, 5}, series.Int, "A"),
		series.New([]int{5, 4, 3, 2, 1}, series.Int, "B"),
		series.New([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, series.Float, "C"),
		series.New([]string{"a", "b", "c", "d", "e"}, series.String, "D"),
		series.New([]int{3, 3, 4, 3, 3}, series.Int, "F"),
	)

	tests := []struct {
		name       string
		columns    []string
		newColName string
		expected   series.Series
	}{
		{
			name:       "Single int column",
			columns:    []string{"A"},
			newColName: "A",
			expected:   series.New([]int{1, 2, 3, 4, 5}, series.Int, "A"),
		},
		{
			name:       "Two int columns",
			columns:    []string{"A", "B"},
			newColName: "NEWC",
			expected:   series.New([]int{5, 4, 3, 4, 5}, series.Int, "NEWC"),
		},
		{
			name:       "Three int columns",
			columns:    []string{"A", "B", "F"},
			newColName: "NEWC",
			expected:   series.New([]int{5, 4, 4, 4, 5}, series.Int, "NEWC"),
		},
		{
			name:       "Int and float columns",
			columns:    []string{"A", "C"},
			newColName: "A",
			expected:   series.New([]int{1, 2, 3, 4, 5}, series.Int, "A"),
		},
		{
			name:       "Int and float columns 2",
			columns:    []string{"C", "A"},
			newColName: "NC",
			expected:   series.New([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, series.Float, "NC"),
		},
		{
			name:       "Non-existent column",
			columns:    []string{"E"},
			newColName: "E",
			expected:   series.New(nil, series.Float, ""),
		},
		{
			name:       "String column (should be ignored)",
			columns:    []string{"D"},
			newColName: "D",
			expected:   series.New(nil, series.Float, ""),
		},
		{
			name:       "Mix of valid and invalid columns",
			columns:    []string{"A", "D", "B"},
			newColName: "A",
			expected:   series.New([]int{5, 4, 3, 4, 5}, series.Int, "A"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MaxInColumns(df, tt.newColName, tt.columns...)
			assert.Equal(t, tt.expected.Records(), result.Records(), "Max function returned unexpected result")
		})
	}
}

func TestRowIterator(t *testing.T) {
	// 创建一个测试用的DataFrame
	df := New(
		series.New([]int{1, 2, 3}, series.Int, "A"),
		series.New([]float64{1.1, 2.2, 3.3}, series.Float, "B"),
		series.New([]string{"a", "b", "c"}, series.String, "C"),
	)

	t.Run("Default options", func(t *testing.T) {
		iter := df.RowsIterator()
		expected := []map[string]interface{}{
			{"A": 1, "B": 1.1, "C": "a"},
			{"A": 2, "B": 2.2, "C": "b"},
			{"A": 3, "B": 3.3, "C": "c"},
		}
		for i := 0; i < 3; i++ {
			rowIndex, rowData, ok := iter()
			assert.True(t, ok)
			assert.Equal(t, i, rowIndex)
			assert.Equal(t, expected[i], rowData)
		}
		_, _, ok := iter()
		assert.False(t, ok)
	})

	t.Run("Without row index", func(t *testing.T) {
		iter := df.RowsIterator(WithRowIndex(false))
		for i := 0; i < 3; i++ {
			rowIndex, _, ok := iter()
			assert.True(t, ok)
			assert.Equal(t, -1, rowIndex)
		}
	})

	t.Run("Without row data", func(t *testing.T) {
		iter := df.RowsIterator(WithRowData(false))
		for i := 0; i < 3; i++ {
			_, rowData, ok := iter()
			assert.True(t, ok)
			assert.Empty(t, rowData)
		}
	})

	t.Run("Selected columns", func(t *testing.T) {
		iter := df.RowsIterator(WithSelectedColumns("A", "C"))
		expected := []map[string]interface{}{
			{"A": 1, "C": "a"},
			{"A": 2, "C": "b"},
			{"A": 3, "C": "c"},
		}
		for i := 0; i < 3; i++ {
			_, rowData, ok := iter()
			assert.True(t, ok)
			assert.Equal(t, expected[i], rowData)
		}
	})

	t.Run("Non-existent column", func(t *testing.T) {
		iter := df.RowsIterator(WithSelectedColumns("A", "D"))
		expected := []map[string]interface{}{
			{"A": 1},
			{"A": 2},
			{"A": 3},
		}
		for i := 0; i < 3; i++ {
			_, rowData, ok := iter()
			assert.True(t, ok)
			assert.Equal(t, expected[i], rowData)
		}
	})

	t.Run("Empty DataFrame", func(t *testing.T) {
		emptyDF := New()
		iter := emptyDF.RowsIterator()
		_, _, ok := iter()
		assert.False(t, ok)
	})
}

func TestDistinct(t *testing.T) {
	// 创建一个包含重复行的DataFrame
	df := New(
		series.New([]int{1, 2, 2, 3, 3, 3}, series.Int, "A"),
		series.New([]string{"a", "b", "b", "c", "c", "c"}, series.String, "B"),
	)

	// 执行Distinct操作
	distinctDF := df.Distinct()

	// 验证结果
	expectedDF := New(
		series.New([]int{1, 2, 3}, series.Int, "A"),
		series.New([]string{"a", "b", "c"}, series.String, "B"),
	)

	assert.True(t, distinctDF.Equal(expectedDF), "Distinct DataFrame does not match expected DataFrame")
}

func TestAntiJoin(t *testing.T) {
	// 创建两个DataFrame进行AntiJoin
	df1 := New(
		series.New([]int{1, 2, 3, 4}, series.Int, "ID"),
		series.New([]string{"A", "B", "C", "D"}, series.String, "Name"),
	)

	df2 := New(
		series.New([]int{2, 4, 5}, series.Int, "ID"),
		series.New([]string{"X", "Y", "Z"}, series.String, "Value"),
	)

	// 执行AntiJoin操作
	result := AntiJoin(df1, df2, "ID")

	// 验证结果
	expectedDF := New(
		series.New([]int{1, 3}, series.Int, "ID"),
		series.New([]string{"A", "C"}, series.String, "Name"),
	)

	assert.True(t, result.Equal(expectedDF), "AntiJoin result does not match expected DataFrame")
}

func TestConcat(t *testing.T) {
	// Test case 1: Concatenate empty DataFrames
	t.Run("Empty DataFrames", func(t *testing.T) {
		result := Concat()
		assert.Equal(t, 0, result.Nrow(), "Expected 0 rows for empty concat")
		assert.Equal(t, 0, result.Ncol(), "Expected 0 columns for empty concat")
	})

	// Test case 2: Concatenate single DataFrame
	t.Run("Single DataFrame", func(t *testing.T) {
		df := New(
			series.New([]int{1, 2, 3}, series.Int, "A"),
			series.New([]float64{1.1, 2.2, 3.3}, series.Float, "B"),
		)
		result := Concat(df)
		assert.Equal(t, df.Nrow(), result.Nrow(), "Expected same number of rows")
		assert.Equal(t, df.Ncol(), result.Ncol(), "Expected same number of columns")
	})

	// Test case 3: Concatenate multiple DataFrames
	t.Run("Multiple DataFrames", func(t *testing.T) {
		df1 := New(
			series.New([]int{1, 2}, series.Int, "A"),
			series.New([]float64{1.1, 2.2}, series.Float, "B"),
		)
		df2 := New(
			series.New([]int{3, 4}, series.Int, "A"),
			series.New([]float64{3.3, 4.4}, series.Float, "B"),
		)
		df3 := New(
			series.New([]int{5, 6}, series.Int, "A"),
			series.New([]float64{5.5, 6.6}, series.Float, "B"),
		)

		result := Concat(df1, df2, df3)
		assert.Equal(t, 6, result.Nrow(), "Expected 6 rows after concatenation")
		assert.Equal(t, 2, result.Ncol(), "Expected 2 columns after concatenation")

		expectedA := series.New([]int{1, 2, 3, 4, 5, 6}, series.Int, "A")
		expectedB := series.New([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6}, series.Float, "B")

		assert.True(t, expectedA.Equal(result.Col("A")), "Column A does not match expected values")
		assert.True(t, expectedB.Equal(result.Col("B")), "Column B does not match expected values")
	})

	// Test case 4: Concatenate DataFrames with different columns
	// t.Run("Different Columns", func(t *testing.T) {
	// 	df1 := New(
	// 		series.New([]int{1, 2}, series.Int, "A"),
	// 		series.New([]float64{1.1, 2.2}, series.Float, "B"),
	// 	)
	// 	df2 := New(
	// 		series.New([]int{3, 4}, series.Int, "A"),
	// 		series.New([]string{"three", "four"}, series.String, "C"),
	// 	)

	// 	result := Concat(df1, df2)
	// 	assert.Equal(t, 4, result.Nrow(), "Expected 4 rows after concatenation")
	// 	assert.Equal(t, 3, result.Ncol(), "Expected 3 columns after concatenation")

	// 	expectedA := series.New([]int{1, 2, 3, 4}, series.Int, "A")
	// 	expectedB := series.New([]float64{1.1, 2.2, nil, nil}, series.Float, "B")
	// 	expectedC := series.New([]string{"", "", "three", "four"}, series.String, "C")

	// 	assert.True(t, expectedA.Equal( result.Col("A")), "Column A does not match expected values")
	// 	assert.True(t, expectedB.Equal( result.Col("B")), "Column B does not match expected values")
	// 	assert.True(t, expectedC.Equal( result.Col("C")), "Column C does not match expected values")
	// })
}

func TestCrossJoin(t *testing.T) {
	// Create test DataFrames
	df1 := New(
		series.New([]int{1, 2}, series.Int, "A"),
		series.New([]string{"a", "b"}, series.String, "B"),
	)

	df2 := New(
		series.New([]float64{1.1, 2.2}, series.Float, "C"),
		series.New([]bool{true, false}, series.Bool, "D"),
	)

	// Test case 1: Basic CrossJoin
	t.Run("Basic CrossJoin", func(t *testing.T) {
		result := df1.CrossJoin(df2)

		assert.Equal(t, 4, result.Nrow(), "Expected 4 rows in the result")
		assert.Equal(t, 4, result.Ncol(), "Expected 4 columns in the result")

		expectedNames := []string{"A", "B", "C", "D"}
		assert.Equal(t, expectedNames, result.Names(), "Column names don't match expected")

		// Check some values
		assert.Equal(t, 1, result.Elem(0, 0).Val())
		assert.Equal(t, "a", result.Elem(0, 1).Val())
		assert.Equal(t, 1.1, result.Elem(0, 2).Val())
		assert.Equal(t, true, result.Elem(0, 3).Val())

		assert.Equal(t, 2, result.Elem(3, 0).Val())
		assert.Equal(t, "b", result.Elem(3, 1).Val())
		assert.Equal(t, 2.2, result.Elem(3, 2).Val())
		assert.Equal(t, false, result.Elem(3, 3).Val())
	})

	// Test case 2: CrossJoin with name conflicts
	t.Run("CrossJoin with name conflicts", func(t *testing.T) {
		df3 := New(
			series.New([]int{3, 4}, series.Int, "A"),
			series.New([]string{"c", "d"}, series.String, "E"),
		)

		result := df1.CrossJoin(df3, WithRightSuffix("_right"))

		assert.Equal(t, 4, result.Nrow(), "Expected 4 rows in the result")
		assert.Equal(t, 4, result.Ncol(), "Expected 4 columns in the result")

		expectedNames := []string{"A", "B", "A_right", "E"}
		assert.Equal(t, expectedNames, result.Names(), "Column names don't match expected")

		// Check some values
		assert.Equal(t, 1, result.Elem(0, 0).Val())
		assert.Equal(t, "a", result.Elem(0, 1).Val())
		assert.Equal(t, 3, result.Elem(0, 2).Val())
		assert.Equal(t, "c", result.Elem(0, 3).Val())

		assert.Equal(t, 2, result.Elem(3, 0).Val())
		assert.Equal(t, "b", result.Elem(3, 1).Val())
		assert.Equal(t, 4, result.Elem(3, 2).Val())
		assert.Equal(t, "d", result.Elem(3, 3).Val())
	})

	// Test case 3: CrossJoin with empty DataFrame
	t.Run("CrossJoin with empty DataFrame", func(t *testing.T) {
		emptyDF := New()
		result := df1.CrossJoin(emptyDF)

		assert.Equal(t, 0, result.Nrow(), "Expected same number of rows as df1")
		assert.Equal(t, df1.Ncol(), result.Ncol(), "Expected same number of columns as df1")
	})
}
func TestGroupAggregate(t *testing.T) {
	// 创建测试数据
	df := New(
		series.New([]string{"A", "B", "A", "B", "A"}, series.String, "category"),
		series.New([]int{1, 2, 3, 4, 5}, series.Int, "value"),
		series.New([]float64{0.1, 0.2, 0.3, 0.4, 0.5}, series.Float, "pct_overlap"),
	)

	// 测试基本分组聚合
	t.Run("Basic GroupAggregate", func(t *testing.T) {
		result := GroupAggregate(df, GroupOn([]string{"category"}...), AggreateOn([]AggregationType{Aggregation_MEAN, Aggregation_MAX}, []string{"value", "pct_overlap"}))

		expected := New(
			series.New([]string{"A", "B"}, series.String, "category"),
			// series.New([]float64{5, 4}, series.Float, "value_max"),
			// series.New([]float64{0.3, 0.3}, series.Float, "pct_overlap_avg"),
			series.New([]float64{0.5, 0.4}, series.Float, "pct_overlap_MAX"),
			series.New([]float64{3, 3}, series.Float, "value_MEAN"),
		)

		assert.True(t, expected.Equal(result))
		// assert.Equal(t, expected.Names(), result.Names())
		// assert.Equal(t, expected.Records(), result.Records())
	})

	t.Run("GroupAggregate with LeftJoin", func(t *testing.T) {
		result := GroupAggregate(df,
			GroupOn([]string{"category"}...),
			AggreateOn([]AggregationType{Aggregation_MEAN, Aggregation_MAX}, []string{"value", "pct_overlap"}),
			WithLeftJoin(df, "category"))

		expected := New(
			series.New([]string{"A", "B", "A", "B", "A"}, series.String, "category"),
			series.New([]int{1, 2, 3, 4, 5}, series.Int, "value"),
			series.New([]float64{0.1, 0.2, 0.3, 0.4, 0.5}, series.Float, "pct_overlap"),
			series.New([]float64{0.5, 0.4, 0.5, 0.4, 0.5}, series.Float, "pct_overlap_MAX"),
			series.New([]float64{3, 3, 3, 3, 3}, series.Float, "value_MEAN"),
		)

		assert.Equal(t, expected.Names(), result.Names())
		assert.Equal(t, expected.Records(), result.Records())
	})

	// // 测试带有多个聚合函数的 GroupAggregate
	// t.Run("GroupAggregate with multiple aggregations", func(t *testing.T) {
	// 	result := GroupAggregate(df, []string{"category"}, []AggregationType{Aggregation_MEAN, Aggregation_MAX, Aggregation_MIN}, []string{"value", "pct_overlap"})

	// 	expected := New(
	// 		series.New([]string{"A", "B"}, series.String, "category"),
	// 		series.New([]float64{3, 3}, series.Float, "value_MEAN"),
	// 		// series.New([]float64{5, 4}, series.Float, "value_max"),
	// 		// series.New([]float64{1, 2}, series.Float, "value_min"),
	// 		// series.New([]float64{0.3, 0.3}, series.Float, "pct_overlap_avg"),
	// 		series.New([]float64{0.5, 0.4}, series.Float, "pct_overlap_MAX"),
	// 		// series.New([]float64{0.1, 0.2}, series.Float, "pct_overlap_min"),
	// 	)

	// 	assert.Equal(t, expected.Names(), result.Names())
	// 	assert.Equal(t, expected.Records(), result.Records())
	// })
}
