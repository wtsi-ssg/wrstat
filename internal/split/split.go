package split

// SplitsToSplitFn returns a simple implementation of the function passed to
// Tree.Where.
func SplitsToSplitFn(splits int) func(string) int {
	return func(_ string) int {
		return splits
	}
}
