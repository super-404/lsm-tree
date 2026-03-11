package engine

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkLSMEngineRandomMixedOps 基准测试混合读写负载（Put/Delete/Write/Get）。
func BenchmarkLSMEngineRandomMixedOps(b *testing.B) {
	raw, err := Open("", nil)
	if err != nil {
		b.Fatalf("Open error: %v", err)
	}
	defer raw.Close()

	rng := rand.New(rand.NewSource(20260311))
	const keySpace = 10000

	valuePayload := make([]byte, 4<<10) // 4KB
	for i := range valuePayload {
		valuePayload[i] = byte('a' + i%26)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := rng.Intn(100)
		key := []byte(fmt.Sprintf("bk-%05d", rng.Intn(keySpace)))

		switch {
		case op < 45:
			v := append([]byte(nil), valuePayload...)
			v = append(v, byte(i%251))
			if err := raw.Put(key, v); err != nil {
				b.Fatalf("Put error: %v", err)
			}
		case op < 65:
			if err := raw.Delete(key); err != nil {
				b.Fatalf("Delete error: %v", err)
			}
		case op < 85:
			batch := raw.NewBatch()
			n := 1 + rng.Intn(4)
			for j := 0; j < n; j++ {
				k := []byte(fmt.Sprintf("bk-%05d", rng.Intn(keySpace)))
				if rng.Intn(10) < 7 {
					v := append([]byte(nil), valuePayload...)
					v = append(v, byte((i+j)%251))
					batch.Put(k, v)
				} else {
					batch.Delete(k)
				}
			}
			if err := raw.Write(batch); err != nil {
				b.Fatalf("Write(batch) error: %v", err)
			}
		default:
			_, _ = raw.Get(key)
		}
	}
}
