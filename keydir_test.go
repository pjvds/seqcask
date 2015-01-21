package seqcask

import "testing"

func BenchmarkKeydirAdds(b *testing.B) {
	keydir := NewKeydir()

	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		keydir.Add(uint64(iteration), 0, 0, 0)
	}
	b.StopTimer()
}
