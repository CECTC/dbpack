package sequence

import (
	"fmt"
	"testing"
)

func BenchmarkSnowFlakeNextID(b *testing.B) {
	worker, err := NewWorker(10)
	if err != nil {
		b.Error(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id, err := worker.NextID()
			if err != nil {
				b.Error(err)
			}
			fmt.Println(id)
		}
	})
}
