package stats

import (
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
)

// ConfidenceInterval holds the result of a bootstrap confidence interval estimate.
type ConfidenceInterval struct {
	Lower         float64
	Upper         float64
	PointEstimate float64
}

// Bootstrap computes a bootstrap confidence interval for any scalar statistic.
//
// fn is applied to each bootstrap resample to produce a scalar.
// data is the original sample.
// nBoot is the number of bootstrap resamples.
// alpha is the significance level (e.g., 0.05 for 95% CI).
//
// Resamples are distributed across GOMAXPROCS goroutines.
// The interval uses the percentile method: lower = alpha/2 quantile, upper = 1-alpha/2 quantile.
func Bootstrap(fn func([]float64) float64, data []float64, nBoot int, alpha float64) ConfidenceInterval {
	pointEstimate := fn(data)
	if nBoot <= 0 || len(data) == 0 {
		return ConfidenceInterval{
			Lower:         pointEstimate,
			Upper:         pointEstimate,
			PointEstimate: pointEstimate,
		}
	}

	nWorkers := runtime.GOMAXPROCS(0)
	if nWorkers > nBoot {
		nWorkers = nBoot
	}

	bootStats := make([]float64, nBoot)
	var wg sync.WaitGroup
	chunkSize := (nBoot + nWorkers - 1) / nWorkers

	for w := 0; w < nWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > nBoot {
			end = nBoot
		}
		if start >= end {
			break
		}
		wg.Add(1)
		go func(start, end int, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			n := len(data)
			resample := make([]float64, n)
			for i := start; i < end; i++ {
				for j := 0; j < n; j++ {
					resample[j] = data[rng.Intn(n)]
				}
				bootStats[i] = fn(resample)
			}
		}(start, end, int64(w+1)*7919)
	}
	wg.Wait()

	sort.Float64s(bootStats)

	loIdx := int(math.Floor(float64(nBoot) * alpha / 2))
	hiIdx := int(math.Ceil(float64(nBoot)*(1-alpha/2))) - 1
	if loIdx < 0 {
		loIdx = 0
	}
	if hiIdx >= nBoot {
		hiIdx = nBoot - 1
	}

	return ConfidenceInterval{
		Lower:         bootStats[loIdx],
		Upper:         bootStats[hiIdx],
		PointEstimate: pointEstimate,
	}
}
