package skiplist

import "math"

type Random struct {
	seed uint32
}

func NewRandom(s uint32) *Random {
	rnd := &Random{seed: s & 0x7fffffff}
	// Avoid bad seed
	if rnd.seed == 0 || rnd.seed == math.MaxInt32 {
		rnd.seed = 1
	}
	return rnd
}

func (r *Random) Next() uint32 {
	const (
		M uint32 = math.MaxInt32
		A uint64 = 16807 // bits 14, 8, 7, 5, 2, 1, 0
	)
	// We are computing
	//       seed_ = (seed_ * A) % M,    where M = 2^31-1
	//
	// seed_ must not be zero or M, or else all subsequent computed values
	// will be zero or M respectively.  For all other values, seed_ will end
	// up cycling through every number in [1,M-1]
	var product uint64 = uint64(r.seed) * A

	// Compute (product % M) using the fact that ((x << 31) % M) == x.
	r.seed = uint32((product >> 31) + (product & uint64(M)))

	// The first reduction may overflow by 1 bit, so we may need to
	// repeat.  mod == M is not possible; using > allows the faster
	// sign-bit-based test.
	if r.seed > M {
		r.seed -= M
	}
	return r.seed
}

// Uniform returns a uniformly distributed value in the range [0..n-1]
// REQUIRES: n > 0
func (r *Random) Uniform(n uint32) uint32 {
	return r.Next() % n
}

// OneIn randomly returns true ~"1/n" of the time, and false otherwise.
// REQUIRES: n > 0
func (r *Random) OneIn(n uint32) bool {
	return (r.Next() % n) == 0
}

// Skewed: pick "base" uniformly from range [0,max_log] and then
// return "base" random bits.  The effect is to pick a number in the
// range [0,2^max_log-1] with exponential bias towards smaller numbers.
func (r *Random) Skewed(max_log uint32) uint32 {
	return r.Uniform(1 << r.Uniform(max_log+1))
}
