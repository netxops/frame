package series

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"math"

	"github.com/spf13/cast"
	"gonum.org/v1/gonum/stat"
)

// Series is a data structure designed for operating on arrays of elements that
// should comply with a certain type structure. They are flexible enough that can
// be transformed to other Series types and account for missing or non valid
// elements. Most of the power of Series resides on the ability to compare and
// subset Series of different types.
type Series struct {
	Name     string   // The name of the series
	elements Elements // The values of the elements
	t        Type     // The type of the series

	// deprecated: use Error() instead
	Err error
}

// Elements is the interface that represents the array of elements contained on
// a Series.
type Elements interface {
	Elem(int) Element
	Len() int
}

// Element is the interface that defines the types of methods to be present for
// elements of a Series
type Element interface {
	// Setter method
	Set(interface{})

	// Comparation methods
	Eq(Element) bool
	Neq(Element) bool
	Less(Element) bool
	LessEq(Element) bool
	Greater(Element) bool
	GreaterEq(Element) bool

	// Accessor/conversion methods
	Copy() Element     // FIXME: Returning interface is a recipe for pain
	Val() ElementValue // FIXME: Returning interface is a recipe for pain
	String() string
	Int() (int, error)
	Float() float64
	Bool() (bool, error)

	// Information methods
	IsNA() bool
	Type() Type
}

// intElements is the concrete implementation of Elements for Int elements.
type intElements []intElement

func (e intElements) Len() int           { return len(e) }
func (e intElements) Elem(i int) Element { return &e[i] }

// stringElements is the concrete implementation of Elements for String elements.
type stringElements []stringElement

func (e stringElements) Len() int           { return len(e) }
func (e stringElements) Elem(i int) Element { return &e[i] }

// floatElements is the concrete implementation of Elements for Float elements.
type floatElements []floatElement

func (e floatElements) Len() int           { return len(e) }
func (e floatElements) Elem(i int) Element { return &e[i] }

// boolElements is the concrete implementation of Elements for Bool elements.
type boolElements []boolElement

func (e boolElements) Len() int           { return len(e) }
func (e boolElements) Elem(i int) Element { return &e[i] }

// ElementValue represents the value that can be used for marshaling or
// unmarshaling Elements.
type ElementValue interface{}

type MapFunction func(Element) Element

// Comparator is a convenience alias that can be used for a more type safe way of
// reason and use comparators.
type Comparator string

// Supported Comparators
const (
	Eq        Comparator = "=="   // Equal
	Neq       Comparator = "!="   // Non equal
	Greater   Comparator = ">"    // Greater than
	GreaterEq Comparator = ">="   // Greater or equal than
	Less      Comparator = "<"    // Lesser than
	LessEq    Comparator = "<="   // Lesser or equal than
	In        Comparator = "in"   // Inside
	CompFunc  Comparator = "func" // user-defined comparison function
)

// compFunc defines a user-defined comparator function. Used internally for type assertions
type compFunc = func(el Element) bool

// Type is a convenience alias that can be used for a more type safe way of
// reason and use Series types.
type Type string

// Supported Series Types
const (
	String Type = "string"
	Int    Type = "int"
	Float  Type = "float"
	Bool   Type = "bool"
)

// Indexes represent the elements that can be used for selecting a subset of
// elements within a Series. Currently supported are:
//
//	int            // Matches the given index number
//	[]int          // Matches all given index numbers
//	[]bool         // Matches all elements in a Series marked as true
//	Series [Int]   // Same as []int
//	Series [Bool]  // Same as []bool
type Indexes interface{}

// New is the generic Series constructor
func New(values interface{}, t Type, name string) Series {
	ret := Series{
		Name: name,
		t:    t,
	}

	// Pre-allocate elements
	preAlloc := func(n int) {
		switch t {
		case String:
			ret.elements = make(stringElements, n)
		case Int:
			ret.elements = make(intElements, n)
		case Float:
			ret.elements = make(floatElements, n)
		case Bool:
			ret.elements = make(boolElements, n)
		default:
			panic(fmt.Sprintf("unknown type %v", t))
		}
	}

	if values == nil {
		preAlloc(1)
		ret.elements.Elem(0).Set(nil)
		return ret
	}

	switch v := values.(type) {
	case []string:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []float64:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []int:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []bool:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case Series:
		l := v.Len()
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v.elements.Elem(i))
		}
	default:
		switch reflect.TypeOf(values).Kind() {
		case reflect.Slice:
			v := reflect.ValueOf(values)
			l := v.Len()
			preAlloc(v.Len())
			for i := 0; i < l; i++ {
				val := v.Index(i).Interface()
				ret.elements.Elem(i).Set(val)
			}
		default:
			preAlloc(1)
			v := reflect.ValueOf(values)
			val := v.Interface()
			ret.elements.Elem(0).Set(val)
		}
	}

	return ret
}

// Strings is a constructor for a String Series
func Strings(values interface{}) Series {
	return New(values, String, "")
}

// Ints is a constructor for an Int Series
func Ints(values interface{}) Series {
	return New(values, Int, "")
}

// Floats is a constructor for a Float Series
func Floats(values interface{}) Series {
	return New(values, Float, "")
}

// Bools is a constructor for a Bool Series
func Bools(values interface{}) Series {
	return New(values, Bool, "")
}

// Empty returns an empty Series of the same type
func (s Series) Empty() Series {
	return New([]int{}, s.t, s.Name)
}

// Returns Error or nil if no error occured
func (s *Series) Error() error {
	return s.Err
}

// Append adds new elements to the end of the Series. When using Append, the
// Series is modified in place.
func (s *Series) Append(values interface{}) {
	if err := s.Err; err != nil {
		return
	}
	news := New(values, s.t, s.Name)
	switch s.t {
	case String:
		s.elements = append(s.elements.(stringElements), news.elements.(stringElements)...)
	case Int:
		s.elements = append(s.elements.(intElements), news.elements.(intElements)...)
	case Float:
		s.elements = append(s.elements.(floatElements), news.elements.(floatElements)...)
	case Bool:
		s.elements = append(s.elements.(boolElements), news.elements.(boolElements)...)
	}
}

// Concat concatenates two series together. It will return a new Series with the
// combined elements of both Series.
func (s Series) Concat(x Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := x.Err; err != nil {
		s.Err = fmt.Errorf("concat error: argument has errors: %v", err)
		return s
	}
	y := s.Copy()
	y.Append(x)
	return y
}

// Subset returns a subset of the series based on the given Indexes.
func (s Series) Subset(indexes Indexes) Series {
	if err := s.Err; err != nil {
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	ret := Series{
		Name: s.Name,
		t:    s.t,
	}
	switch s.t {
	case String:
		elements := make(stringElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(stringElements)[i]
		}
		ret.elements = elements
	case Int:
		elements := make(intElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(intElements)[i]
		}
		ret.elements = elements
	case Float:
		elements := make(floatElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(floatElements)[i]
		}
		ret.elements = elements
	case Bool:
		elements := make(boolElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(boolElements)[i]
		}
		ret.elements = elements
	default:
		panic("unknown series type")
	}
	return ret
}

// Set sets the values on the indexes of a Series and returns the reference
// for itself. The original Series is modified.
func (s Series) Set(indexes Indexes, newvalues Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := newvalues.Err; err != nil {
		s.Err = fmt.Errorf("set error: argument has errors: %v", err)
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	if len(idx) != newvalues.Len() {
		s.Err = fmt.Errorf("set error: dimensions mismatch")
		return s
	}
	for k, i := range idx {
		if i < 0 || i >= s.Len() {
			s.Err = fmt.Errorf("set error: index out of range")
			return s
		}
		s.elements.Elem(i).Set(newvalues.elements.Elem(k))
	}
	return s
}

func (s Series) NewFill(value interface{}, t Type, name string) Series {
	valueList, ok := prepareValueList(value, s.Len(), t)
	if !ok {
		return Series{
			Name: name,
			t:    t,
			Err:  fmt.Errorf("newfill error: value type mismatch"),
		}
	}

	return New(valueList, t, name)
}

func prepareValueList(value interface{}, n int, t Type) (interface{}, bool) {
	var valueList interface{}
	switch t {
	case String:
		s, ok := value.(string)
		if ok {
			valueList = make([]string, n)
			for i := 0; i < n; i++ {
				valueList.([]string)[i] = s
			}
		}
		return valueList, ok
	case Int:
		ii, ok := value.(int)
		if ok {
			valueList = make([]int, n)
			for i := 0; i < n; i++ {
				valueList.([]int)[i] = ii
			}
		}
		// 也接受可以转换为int的类型
		return valueList, ok
	case Float:
		f, ok := value.(float64)
		if ok {
			valueList = make([]float64, n)
			for i := 0; i < n; i++ {
				valueList.([]float64)[i] = f
			}
		}
		// 也接受int，因为int可以安全地转换为float
		return valueList, ok
	case Bool:
		b, ok := value.(bool)
		if ok {
			valueList = make([]bool, n)
			for i := 0; i < n; i++ {
				valueList.([]bool)[i] = b
			}
		}
		return value, ok
	default:
		return value, false
	}
}

// HasNaN checks whether the Series contain NaN elements.
func (s Series) HasNaN() bool {
	for i := 0; i < s.Len(); i++ {
		if s.elements.Elem(i).IsNA() {
			return true
		}
	}
	return false
}

// IsNaN returns an array that identifies which of the elements are NaN.
func (s Series) IsNaN() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.elements.Elem(i).IsNA()
	}
	return ret
}

// Compare compares the values of a Series with other elements. To do so, the
// elements with are to be compared are first transformed to a Series of the same
// type as the caller.
func (s Series) Compare(comparator Comparator, comparando interface{}) Series {
	if err := s.Err; err != nil {
		return s
	}
	compareElements := func(a, b Element, c Comparator) (bool, error) {
		var ret bool
		switch c {
		case Eq:
			ret = a.Eq(b)
		case Neq:
			ret = a.Neq(b)
		case Greater:
			ret = a.Greater(b)
		case GreaterEq:
			ret = a.GreaterEq(b)
		case Less:
			ret = a.Less(b)
		case LessEq:
			ret = a.LessEq(b)
		default:
			return false, fmt.Errorf("unknown comparator: %v", c)
		}
		return ret, nil
	}

	bools := make([]bool, s.Len())

	// CompFunc comparator comparison
	if comparator == CompFunc {
		f, ok := comparando.(compFunc)
		if !ok {
			panic("comparando is not a comparison function of type func(el Element) bool")
		}

		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			bools[i] = f(e)
		}

		return Bools(bools)
	}

	comp := New(comparando, s.t, "")
	// In comparator comparison
	if comparator == In {
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			b := false
			for j := 0; j < comp.Len(); j++ {
				m := comp.elements.Elem(j)
				c, err := compareElements(e, m, Eq)
				if err != nil {
					s = s.Empty()
					s.Err = err
					return s
				}
				if c {
					b = true
					break
				}
			}
			bools[i] = b
		}
		return Bools(bools)
	}

	// Single element comparison
	if comp.Len() == 1 {
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			c, err := compareElements(e, comp.elements.Elem(0), comparator)
			if err != nil {
				s = s.Empty()
				s.Err = err
				return s
			}
			bools[i] = c
		}
		return Bools(bools)
	}

	// Multiple element comparison
	if s.Len() != comp.Len() {
		s := s.Empty()
		s.Err = fmt.Errorf("can't compare: length mismatch")
		return s
	}
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		c, err := compareElements(e, comp.elements.Elem(i), comparator)
		if err != nil {
			s = s.Empty()
			s.Err = err
			return s
		}
		bools[i] = c
	}
	return Bools(bools)
}

// Copy will return a copy of the Series.
func (s Series) Copy() Series {
	name := s.Name
	t := s.t
	err := s.Err
	var elements Elements
	switch s.t {
	case String:
		elements = make(stringElements, s.Len())
		copy(elements.(stringElements), s.elements.(stringElements))
	case Float:
		elements = make(floatElements, s.Len())
		copy(elements.(floatElements), s.elements.(floatElements))
	case Bool:
		elements = make(boolElements, s.Len())
		copy(elements.(boolElements), s.elements.(boolElements))
	case Int:
		elements = make(intElements, s.Len())
		copy(elements.(intElements), s.elements.(intElements))
	}
	ret := Series{
		Name:     name,
		t:        t,
		elements: elements,
		Err:      err,
	}
	return ret
}

// Records returns the elements of a Series as a []string
func (s Series) Records() []string {
	ret := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.String()
	}
	return ret
}

// Float returns the elements of a Series as a []float64. If the elements can not
// be converted to float64 or contains a NaN returns the float representation of
// NaN.
func (s Series) Float() []float64 {
	ret := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.Float()
	}
	return ret
}

// Int returns the elements of a Series as a []int or an error if the
// transformation is not possible.
func (s Series) Int() ([]int, error) {
	ret := make([]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Int()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Bool returns the elements of a Series as a []bool or an error if the
// transformation is not possible.
func (s Series) Bool() ([]bool, error) {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Bool()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Type returns the type of a given series
func (s Series) Type() Type {
	return s.t
}

// Len returns the length of a given Series
func (s Series) Len() int {
	return s.elements.Len()
}

// String implements the Stringer interface for Series
func (s Series) String() string {
	return fmt.Sprint(s.elements)
}

// Str prints some extra information about a given series
func (s Series) Str() string {
	var ret []string
	// If name exists print name
	if s.Name != "" {
		ret = append(ret, "Name: "+s.Name)
	}
	ret = append(ret, "Type: "+fmt.Sprint(s.t))
	ret = append(ret, "Length: "+fmt.Sprint(s.Len()))
	if s.Len() != 0 {
		ret = append(ret, "Values: "+fmt.Sprint(s))
	}
	return strings.Join(ret, "\n")
}

// Val returns the value of a series for the given index. Will panic if the index
// is out of bounds.
func (s Series) Val(i int) interface{} {
	return s.elements.Elem(i).Val()
}

// Elem returns the element of a series for the given index. Will panic if the
// index is out of bounds.
func (s Series) Elem(i int) Element {
	return s.elements.Elem(i)
}

// parseIndexes will parse the given indexes for a given series of length `l`. No
// out of bounds checks is performed.
func parseIndexes(l int, indexes Indexes) ([]int, error) {
	var idx []int
	switch idxs := indexes.(type) {
	case []int:
		idx = idxs
	case int:
		idx = []int{idxs}
	case []bool:
		bools := idxs
		if len(bools) != l {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case Series:
		s := idxs
		if err := s.Err; err != nil {
			return nil, fmt.Errorf("indexing error: new values has errors: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("indexing error: indexes contain NaN")
		}
		switch s.t {
		case Int:
			return s.Int()
		case Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("indexing error: %v", err)
			}
			return parseIndexes(l, bools)
		default:
			return nil, fmt.Errorf("indexing error: unknown indexing mode")
		}
	default:
		return nil, fmt.Errorf("indexing error: unknown indexing mode")
	}
	return idx, nil
}

// Order returns the indexes for sorting a Series. NaN elements are pushed to the
// end by order of appearance.
func (s Series) Order(reverse bool) []int {
	var ie indexedElements
	var nasIdx []int
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		if e.IsNA() {
			nasIdx = append(nasIdx, i)
		} else {
			ie = append(ie, indexedElement{i, e})
		}
	}
	var srt sort.Interface
	srt = ie
	if reverse {
		srt = sort.Reverse(srt)
	}
	sort.Stable(srt)
	var ret []int
	for _, e := range ie {
		ret = append(ret, e.index)
	}
	return append(ret, nasIdx...)
}

type indexedElement struct {
	index   int
	element Element
}

type indexedElements []indexedElement

func (e indexedElements) Len() int           { return len(e) }
func (e indexedElements) Less(i, j int) bool { return e[i].element.Less(e[j].element) }
func (e indexedElements) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

// StdDev calculates the standard deviation of a series
func (s Series) StdDev() float64 {
	stdDev := stat.StdDev(s.Float(), nil)
	return stdDev
}

// Mean calculates the average value of a series
func (s Series) Mean() float64 {
	stdDev := stat.Mean(s.Float(), nil)
	return stdDev
}

// Median calculates the middle or median value, as opposed to
// mean, and there is less susceptible to being affected by outliers.
func (s Series) Median() float64 {
	if s.elements.Len() == 0 ||
		s.Type() == String ||
		s.Type() == Bool {
		return math.NaN()
	}
	ix := s.Order(false)
	newElem := make([]Element, len(ix))

	for newpos, oldpos := range ix {
		newElem[newpos] = s.elements.Elem(oldpos)
	}

	// When length is odd, we just take length(list)/2
	// value as the median.
	if len(newElem)%2 != 0 {
		return newElem[len(newElem)/2].Float()
	}
	// When length is even, we take middle two elements of
	// list and the median is an average of the two of them.
	return (newElem[(len(newElem)/2)-1].Float() +
		newElem[len(newElem)/2].Float()) * 0.5
}

// Max return the biggest element in the series
func (s Series) Max() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.Float()
}

// MaxStr return the biggest element in a series of type String
func (s Series) MaxStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.String()
}

// Min return the lowest element in the series
func (s Series) Min() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.Float()
}

// MinStr return the lowest element in a series of type String
func (s Series) MinStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.String()
}

// Quantile returns the sample of x such that x is greater than or
// equal to the fraction p of samples.
// Note: gonum/stat panics when called with strings
func (s Series) Quantile(p float64) float64 {
	if s.Type() == String || s.Len() == 0 {
		return math.NaN()
	}

	ordered := s.Subset(s.Order(false)).Float()

	return stat.Quantile(p, stat.Empirical, ordered, nil)
}

// Map applies a function matching MapFunction signature, which itself
// allowing for a fairly flexible MAP implementation, intended for mapping
// the function over each element in Series and returning a new Series object.
// Function must be compatible with the underlying type of data in the Series.
// In other words it is expected that when working with a Float Series, that
// the function passed in via argument `f` will not expect another type, but
// instead expects to handle Element(s) of type Float.
func (s Series) Map(f MapFunction) Series {
	mappedValues := make([]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		value := f(s.elements.Elem(i))
		mappedValues[i] = value
	}
	return New(mappedValues, s.Type(), s.Name)
}

// Sum calculates the sum value of a series
func (s Series) Sum() float64 {
	if s.elements.Len() == 0 || s.Type() == String || s.Type() == Bool {
		return math.NaN()
	}
	sFloat := s.Float()
	sum := sFloat[0]
	for i := 1; i < len(sFloat); i++ {
		elem := sFloat[i]
		sum += elem
	}
	return sum
}

// Slice slices Series from j to k-1 index.
func (s Series) Slice(j, k int) Series {
	if s.Err != nil {
		return s
	}

	if j > k || j < 0 || k >= s.Len() {
		empty := s.Empty()
		empty.Err = fmt.Errorf("slice index out of bounds")
		return empty
	}

	idxs := make([]int, k-j)
	for i := 0; j+i < k; i++ {
		idxs[i] = j + i
	}

	return s.Subset(idxs)
}

// Equal compares two Series for equality.
// Two Series are considered equal if they have the same name, type, length,
// and all elements are equal.
func (s Series) Equal(other Series) bool {
	if s.Name != other.Name || s.t != other.t || s.Len() != other.Len() {
		return false
	}

	for i := 0; i < s.Len(); i++ {
		if !s.elements.Elem(i).Eq(other.elements.Elem(i)) {
			return false
		}
	}

	return true
}

// ValuesOptions represents options for the ValuesIterator
type ValuesOptions struct {
	Step       int  // Step size for iteration (default: 1)
	Reverse    bool // Iterate in reverse order
	SkipNaN    bool // Skip NaN values
	OnlyUnique bool // Return only unique values
}

func WithStep(step int) IteratorOption {
	return func(opts *ValuesOptions) {
		opts.Step = step
	}
}

func WithReverse(reverse bool) IteratorOption {
	return func(opts *ValuesOptions) {
		opts.Reverse = reverse
	}
}

func WithSkipNaN(skipNaN bool) IteratorOption {
	return func(opts *ValuesOptions) {
		opts.SkipNaN = skipNaN
	}
}

func WithOnlyUnique(onlyUnique bool) IteratorOption {
	return func(opts *ValuesOptions) {
		opts.OnlyUnique = onlyUnique
	}
}

type IteratorOption func(*ValuesOptions)
type iterator func() (int, interface{}, bool)

// ValuesIterator returns an iterator function for the values in the Series.
func (s Series) ValuesIterator(opts ...IteratorOption) iterator {
	options := ValuesOptions{Step: 1}

	for _, opt := range opts {
		opt(&options)
	}
	if options.Step == 0 {
		options.Step = 1
	}
	index := 0
	if options.Reverse {
		index = s.Len() - 1
	}

	seen := make(map[interface{}]bool)

	return func() (int, interface{}, bool) {
		for {
			if options.Reverse {
				if index < 0 {
					return -1, nil, false
				}
			} else {
				if index >= s.Len() {
					return -1, nil, false
				}
			}

			value := s.Val(index)

			if options.SkipNaN && s.elements.Elem(index).IsNA() {
				if options.Reverse {
					index -= options.Step
				} else {
					index += options.Step
				}
				continue
			}

			if options.OnlyUnique {
				if _, exists := seen[value]; exists {
					if options.Reverse {
						index -= options.Step
					} else {
						index += options.Step
					}
					continue
				}
				seen[value] = true
			}

			currentIndex := index
			if options.Reverse {
				index -= options.Step
			} else {
				index += options.Step
			}

			return currentIndex, value, true
		}
	}
}

func NewFromIterator(it iterator, name string) Series {
	index := 0
	var result Series
	for _, v, ok := it(); ok; _, v, ok = it() {
		var t Type
		if index == 0 {
			switch v.(type) {
			case float64:
				t = Float
			case string:
				t = String
			case bool:
				t = Bool
			default:
				t = String
			}
			result = New(v, t, name)
		} else {
			result.Append(v)
		}
		index++
	}

	return result
}

// Number is a constraint that permits any number type
type Number interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// arithmeticOp defines the signature for arithmetic operations
type arithmeticOp func(a, b float64) float64

// Add performs addition with the given value or Series
func (s Series) Add(value interface{}, name string) Series {
	return arithmeticOperation(s, value, "add", name)
}

// Sub performs subtraction with the given value or Series
func (s Series) Sub(value interface{}, name string) Series {
	return arithmeticOperation(s, value, "sub", name)
}

// Mul performs multiplication with the given value or Series
func (s Series) Mul(value interface{}, name string) Series {
	return arithmeticOperation(s, value, "mul", name)
}

// Div performs division with the given value or Series
func (s Series) Div(value interface{}, name string) Series {
	return arithmeticOperation(s, value, "div", name)
}

// performArithmetic is a generic function to perform arithmetic operations
func performArithmetic(s Series, value interface{}, op string, name string) Series {
	if s.Type() != Int && s.Type() != Float {
		s.Err = fmt.Errorf("cannot perform arithmetic operation on series of type %s", s.Type())
		return s
	}
	var finalType Type
	rt := reflect.TypeOf(value)
	var emptyList interface{}
	switch rt.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if s.Type() == Int {
			finalType = Int
			emptyList = make([]int, s.Len())
		} else {
			finalType = Float
			emptyList = make([]float64, s.Len())
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if s.Type() == Int {
			finalType = Int
			emptyList = make([]int, s.Len())
		} else {
			finalType = Float
			emptyList = make([]float64, s.Len())
		}
	case reflect.Float32, reflect.Float64:
		finalType = Float
		emptyList = make([]float64, s.Len())
	default:
		s.Err = fmt.Errorf("invalid type for arithmetic operation: %T", value)
		return s
	}

	if name == "" {
		name = s.Name + "_" + op + "_" + fmt.Sprintf("%T", value)
	}

	result := New(emptyList, finalType, name)
	for i := 0; i < s.Len(); i++ {
		value, err := operator(s.elements.Elem(i).Val(), value, op, finalType)
		if err != nil {
			s.Err = err
			return s
		}
		result.elements.Elem(i).Set(value)
	}

	return result
}

// performSeriesArithmetic performs arithmetic operations between two Series
func (s Series) performSeriesArithmetic(other Series, op string, name string) Series {
	if s.Err != nil {
		return s
	}
	if other.Err != nil {
		s.Err = other.Err
		return s
	}
	if s.Len() != other.Len() {
		s.Err = fmt.Errorf("cannot perform operation on series of different lengths")
		return s
	}

	// 根据s.Type()和other.Type()判断最终Series的类型
	var emptyList interface{}
	var finalType Type
	if s.Type() == Int && other.Type() == Int {
		finalType = Int
		emptyList = make([]int, s.Len())
	} else if s.Type() == Float && other.Type() == Int {
		finalType = Float
		emptyList = make([]float64, s.Len())
	} else if s.Type() == Int && other.Type() == Float {
		finalType = Float
		emptyList = make([]float64, s.Len())
	} else if s.Type() == Float && other.Type() == Float {
		finalType = Float
		emptyList = make([]float64, s.Len())
	} else {
		s.Err = fmt.Errorf("cannot perform arithmetic operation between series of different types")
		return s
	}

	if name == "" {
		name = s.Name + "_" + op + "_" + other.Name
	}

	result := New(emptyList, finalType, name)
	// result := s.Copy()
	for i := 0; i < s.Len(); i++ {
		value, err := operator(s.elements.Elem(i).Val(), other.elements.Elem(i).Val(), op, finalType)
		if err != nil {
			s.Err = err
			return s
		}
		result.Elem(i).Set(value)
	}
	return result
}

func operator(a, b interface{}, op string, finalType Type) (Element, error) {
	if finalType != Int && finalType != Float {
		return nil, fmt.Errorf("cannot perform arithmetic operation between series of different types")
	}
	// 都转换为float64进行操作，然后根据finalType转换为最终类型
	var aFloat, bFloat float64
	var err error
	switch a := a.(type) {
	case float64:
		aFloat = a
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		aFloat, err = cast.ToFloat64E(a)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %v to float64", a)
		}
	default:
		return nil, fmt.Errorf("unsupported type for arithmetic operation: %v", reflect.TypeOf(a))
	}

	switch b := b.(type) {
	case float64:
		bFloat = b
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		bFloat, err = cast.ToFloat64E(b)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %v to float64", b)
		}
	default:
		return nil, fmt.Errorf("unsupported type for arithmetic operation: %v", reflect.TypeOf(b))
	}

	var value interface{}
	switch op {
	case "add":
		value = aFloat + bFloat
	case "sub":
		value = aFloat - bFloat
	case "mul":
		value = aFloat * bFloat
	case "div":
		if bFloat == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		value = aFloat / bFloat
	default:
		return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
	}

	if finalType == Int {
		return &intElement{
			e: int(value.(float64)),
		}, nil
	}

	return &floatElement{e: value.(float64)}, nil

}

// arithmeticOperation is a helper function to perform arithmetic operations
func arithmeticOperation(s Series, value interface{}, op string, name string) Series {
	if s.Err != nil {
		return s
	}

	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return performArithmetic(s, value, op, name)
	case Series:
		return s.performSeriesArithmetic(v, op, name)
	default:
		s.Err = fmt.Errorf("unsupported type for arithmetic operation: %v", reflect.TypeOf(value))
		return s
	}
}

// compareSeriesHelper is a helper function for comparing multiple series
func compareSeriesHelper(ss []Series, name string, comparator Comparator) Series {
	if len(ss) == 0 {
		return Series{Err: fmt.Errorf("no series provided")}
	}

	// Check if all series have the same length
	length := ss[0].Len()
	for _, s := range ss[1:] {
		if s.Len() != length {
			return Series{Err: fmt.Errorf("all series must have the same length")}
		}
	}

	// Check if all series have comparable types
	var floatSeries *Series
	for i, s := range ss {
		if s.Type() != Int && s.Type() != Float && s.Type() != String {
			return Series{Err: fmt.Errorf("series of type %v cannot be compared", s.Type())}
		}
		if s.Type() == Float {
			floatSeries = &ss[i]
		}
	}

	compareElements := func(a, b Element, c Comparator) (bool, error) {
		var ret bool
		switch c {
		case Eq:
			ret = a.Eq(b)
		case Neq:
			ret = a.Neq(b)
		case Greater:
			ret = a.Greater(b)
		case GreaterEq:
			ret = a.GreaterEq(b)
		case Less:
			ret = a.Less(b)
		case LessEq:
			ret = a.LessEq(b)
		default:
			return false, fmt.Errorf("unknown comparator: %v", c)
		}
		return ret, nil
	}

	// Create a new series to store the result values
	var resultSeries Series
	if floatSeries != nil && floatSeries.Len() > 0 {
		resultSeries = floatSeries.Copy()
	} else {
		resultSeries = ss[0].Copy()
	}
	resultSeries.Name = name

	// Compare values across all series
	for i := 0; i < length; i++ {
		var target Element
		target = ss[0].elements.Elem(i)
		for j := 0; j < len(ss); j++ {
			// a := ss[j-1].elements.Elem(i)
			b := ss[j].elements.Elem(i)
			ret, err := compareElements(target, b, comparator)
			if err != nil {
				return Series{Err: err}
			}
			if !ret {
				target = b
			}
		}
		resultSeries.elements.Elem(i).Set(target)

	}

	return resultSeries
}

// Max returns a new Series with the maximum values from the input Series
func Max(name string, ss ...Series) Series {
	s := compareSeriesHelper(ss, "max", Greater)
	if name != "" {
		s.Name = name

	}
	return s
}

// Min returns a new Series with the minimum values from the input Series
func Min(name string, ss ...Series) Series {
	s := compareSeriesHelper(ss, "min", Less)
	if name != "" {
		s.Name = name

	}
	return s
}
