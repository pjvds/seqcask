package seqcask

import "testing"

func BenchmarkSeqDirAdds(b *testing.B) {
	seqdir := NewSeqDir()

	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		seqdir.Add(uint64(iteration), 0, 0, 0)
	}
	b.StopTimer()
}
