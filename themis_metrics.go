// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package themis

import (
	"github.com/rcrowley/go-metrics"
	"log"
	"os"
	"time"
)

var (
	mtxReg              metrics.Registry
	metricsCounterMap   = make(map[string]metrics.Counter)
	metricsHistogramMap = make(map[string]metrics.Histogram)
)

const (
	metricsGetCounter                    = "themis.get.counter"
	metricsPutCounter                    = "themis.put.counter"
	metricsDeleteCounter                 = "themis.delete.counter"
	metricsClearLockCounter              = "themis.clearLock.counter"
	metricsTxnCommitCounter              = "themis.txnCommit.counter"
	metricsPrewritePrimaryCounter        = "themis.prewritePrimary.counter"
	metricsBatchPrewriteSecondaryCounter = "themis.batchPrewriteSecondary.counter"
	metricsSyncPrewriteSecondaryCounter  = "themis.syncPrewriteSecondary.counter"
	metricsCommitPrimaryCounter          = "themis.commitPrimary.counter"
	metricsBatchCommitSecondaryCounter   = "themis.batchCommitSecondary.counter"
	metricsSyncCommitSecondaryCounter    = "themis.syncCommitSecondary.counter"
	metricsLockRowCounter                = "themis.lockRow.counter"
	metricsRollbackCounter               = "themis.rollback.counter"

	metricsGetTimeSum                    = "themis.get.timeSum"
	metricsClearLockTimeSum              = "themis.clearLock.timeSum"
	metricsTxnCommitTimeSum              = "themis.txnCommit.timeSum"
	metricsPrewritePrimaryTimeSum        = "themis.prewritePrimary.timeSum"
	metricsBatchPrewriteSecondaryTimeSum = "themis.batchPrewriteSecondary.timeSum"
	metricsSyncPrewriteSecondaryTimeSum  = "themis.syncPrewriteSecondary.timeSum"
	metricsCommitPrimaryTimeSum          = "themis.commitPrimary.timeSum"
	metricsBatchCommitSecondaryTimeSum   = "themis.batchCommitSecondary.timeSum"
	metricsSyncCommitSecondaryTimeSum    = "themis.syncCommitSecondary.timeSum"
	metricsLockRowTimeSum                = "themis.lockRow.timeSum"
	metricsRollbackTimeSum               = "themis.rollback.timeSum"

	metricsGetAverageTime                    = "themis.get.averageTime"
	metricsClearLockAverageTime              = "themis.clearLock.averageTime"
	metricsTxnCommitAverageTime              = "themis.txnCommit.averageTime"
	metricsPrewritePrimaryAverageTime        = "themis.prewritePrimary.averageTime"
	metricsBatchPrewriteSecondaryAverageTime = "themis.batchPrewriteSecondary.averageTime"
	metricsSyncPrewriteSecondaryAverageTime  = "themis.syncPrewriteSecondary.averageTime"
	metricsCommitPrimaryAverageTime          = "themis.commitPrimary.averageTime"
	metricsBatchCommitSecondaryAverageTime   = "themis.batchCommitSecondary.averageTime"
	metricsSyncCommitSecondaryAverageTime    = "themis.syncCommitSecondary.averageTime"
	metricsLockRowAverageTime                = "themis.lockRow.averageTime"
	metricsRollbackAverageTime               = "themis.rollback.averageTime"
)

func RegMetircs(r metrics.Registry) {
	if mtxReg != nil {
		return
	}

	mtxReg = r
	metricsCounterMap[metricsGetCounter] = metrics.NewCounter()
	metricsCounterMap[metricsPutCounter] = metrics.NewCounter()
	metricsCounterMap[metricsDeleteCounter] = metrics.NewCounter()
	metricsCounterMap[metricsClearLockCounter] = metrics.NewCounter()
	metricsCounterMap[metricsTxnCommitCounter] = metrics.NewCounter()
	metricsCounterMap[metricsPrewritePrimaryCounter] = metrics.NewCounter()
	metricsCounterMap[metricsBatchPrewriteSecondaryCounter] = metrics.NewCounter()
	metricsCounterMap[metricsSyncPrewriteSecondaryCounter] = metrics.NewCounter()
	metricsCounterMap[metricsCommitPrimaryCounter] = metrics.NewCounter()
	metricsCounterMap[metricsBatchCommitSecondaryCounter] = metrics.NewCounter()
	metricsCounterMap[metricsSyncCommitSecondaryCounter] = metrics.NewCounter()
	metricsCounterMap[metricsLockRowCounter] = metrics.NewCounter()
	metricsCounterMap[metricsRollbackCounter] = metrics.NewCounter()

	metricsCounterMap[metricsGetTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsClearLockTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsTxnCommitTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsPrewritePrimaryTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsBatchPrewriteSecondaryTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsSyncPrewriteSecondaryTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsCommitPrimaryTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsBatchCommitSecondaryTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsSyncCommitSecondaryTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsLockRowTimeSum] = metrics.NewCounter()
	metricsCounterMap[metricsRollbackTimeSum] = metrics.NewCounter()

	metricsHistogramMap[metricsGetAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsClearLockAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsTxnCommitAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsPrewritePrimaryAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsBatchPrewriteSecondaryAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsSyncPrewriteSecondaryAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsCommitPrimaryAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsBatchCommitSecondaryAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsSyncCommitSecondaryAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsLockRowAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))
	metricsHistogramMap[metricsRollbackAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))

	for k, v := range metricsCounterMap {
		mtxReg.Register(k, v)
	}
	for k, v := range metricsHistogramMap {
		mtxReg.Register(k, v)
	}

	//metricsLog(mtxReg, time.Second*10)
	metrics.Log(mtxReg, 30e9, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))
}

// c: call number counter, sumC:call total time counter, h:record call average time
func recordMetrics(c string, sumC string, h string, startTime time.Time) {
	recordCounterMetrics(c, 1)
	recordSumAndAverageTimeMetrics(sumC, h, startTime)
}

// sumC:call total time counter, h:record call average time
func recordSumAndAverageTimeMetrics(sumC string, h string, startTime time.Time) {
	// ms
	d := time.Since(startTime).Nanoseconds() / 1000000
	recordCounterMetrics(sumC, d)
	recordHistogramMetrics(h, d)
}

func recordCounterMetrics(metricsItem string, n int64) {
	c := metricsCounterMap[metricsItem]
	if c != nil {
		c.Inc(n)
	}
}

func recordHistogramMetrics(metricsItem string, n int64) {
	h := metricsHistogramMap[metricsItem]
	if h != nil {
		h.Update(n)
	}
}

//func metricsLog(r metrics.Registry, d time.Duration) {
//	for _ = range time.Tick(d) {
//		r.Each(func(name string, i interface{}) {
//			switch metric := i.(type) {
//			case metrics.Counter:
//				log.Errorf("counter %s\n", name)
//				log.Errorf("  count:       %9d\n", metric.Count())
//			case metrics.Gauge:
//				log.Errorf("gauge %s\n", name)
//				log.Errorf("  value:       %9d\n", metric.Value())
//			case metrics.GaugeFloat64:
//				log.Errorf("gauge %s\n", name)
//				log.Errorf("  value:       %f\n", metric.Value())
//			case metrics.Healthcheck:
//				metric.Check()
//				log.Errorf("healthcheck %s\n", name)
//				log.Errorf("  error:       %v\n", metric.Error())
//			case metrics.Histogram:
//				h := metric.Snapshot()
//				ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
//				log.Errorf("histogram %s\n", name)
//				log.Errorf("  count:       %9d\n", h.Count())
//				log.Errorf("  min:         %9d\n", h.Min())
//				log.Errorf("  max:         %9d\n", h.Max())
//				log.Errorf("  mean:        %12.2f\n", h.Mean())
//				log.Errorf("  stddev:      %12.2f\n", h.StdDev())
//				log.Errorf("  median:      %12.2f\n", ps[0])
//				log.Errorf("  75%%:         %12.2f\n", ps[1])
//				log.Errorf("  95%%:         %12.2f\n", ps[2])
//				log.Errorf("  99%%:         %12.2f\n", ps[3])
//				log.Errorf("  99.9%%:       %12.2f\n", ps[4])
//			case metrics.Meter:
//				m := metric.Snapshot()
//				log.Errorf("meter %s\n", name)
//				log.Errorf("  count:       %9d\n", m.Count())
//				log.Errorf("  1-min rate:  %12.2f\n", m.Rate1())
//				log.Errorf("  5-min rate:  %12.2f\n", m.Rate5())
//				log.Errorf("  15-min rate: %12.2f\n", m.Rate15())
//				log.Errorf("  mean rate:   %12.2f\n", m.RateMean())
//			case metrics.Timer:
//				t := metric.Snapshot()
//				ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
//				log.Errorf("timer %s\n", name)
//				log.Errorf("  count:       %9d\n", t.Count())
//				log.Errorf("  min:         %9d\n", t.Min())
//				log.Errorf("  max:         %9d\n", t.Max())
//				log.Errorf("  mean:        %12.2f\n", t.Mean())
//				log.Errorf("  stddev:      %12.2f\n", t.StdDev())
//				log.Errorf("  median:      %12.2f\n", ps[0])
//				log.Errorf("  75%%:         %12.2f\n", ps[1])
//				log.Errorf("  95%%:         %12.2f\n", ps[2])
//				log.Errorf("  99%%:         %12.2f\n", ps[3])
//				log.Errorf("  99.9%%:       %12.2f\n", ps[4])
//				log.Errorf("  1-min rate:  %12.2f\n", t.Rate1())
//				log.Errorf("  5-min rate:  %12.2f\n", t.Rate5())
//				log.Errorf("  15-min rate: %12.2f\n", t.Rate15())
//				log.Errorf("  mean rate:   %12.2f\n", t.RateMean())
//			}
//		})
//	}
//}
