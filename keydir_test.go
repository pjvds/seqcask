package bitcask

import "testing"

func BenchmarkKeydirAdds(b *testing.B) {
	keydir := NewKeydir()

	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		keydir.Add([]byte{byte(iteration << 0), byte(iteration << 8)}, 0, 0, 0, 0)
	}
	b.StopTimer()
}
