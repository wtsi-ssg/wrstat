package split

func SplitsToSplitFn(splits int) func(string) int {
	return func(path string) int {
		return splits
	}
}
