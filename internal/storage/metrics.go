package storage

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	stageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "klsm",
			Name:      "stage_duration_seconds",
			Help:      "Time spent in each internal stage of an operation.",
			Buckets:   prometheus.ExponentialBucketsRange(10e-6, 10, 20),
		},
		[]string{"op", "stage"},
	)

	getSource = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "klsm",
			Name:      "get_source_total",
			Help:      "Where a GET was resolved: memtable, sstable, tombstone or not_found.",
		},
		[]string{"source"},
	)

	sstableFilesScanned = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "klsm",
			Name:      "sstable_files_scanned_total",
			Help:      "Number of SSTable files opened while serving GETs.",
		},
	)

	memtableEntries = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "klsm",
			Name:      "memtable_entries",
			Help:      "Entries currently held in the memtable.",
		},
	)

	sstablesPerLevel = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "klsm",
			Name:      "sstables",
			Help:      "Number of SSTable files per level.",
		},
		[]string{"level"},
	)

	flushTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "klsm",
			Name:      "flush_total",
			Help:      "Memtable flushes to level 0.",
		},
	)

	compactionTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "klsm",
			Name:      "compaction_total",
			Help:      "SSTable compactions run.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		stageDuration,
		getSource,
		sstableFilesScanned,
		memtableEntries,
		sstablesPerLevel,
		flushTotal,
		compactionTotal,
	)
}

// observeStage records the time elapsed since start for one stage of an operation.
func observeStage(op, stage string, start time.Time) {
	stageDuration.WithLabelValues(op, stage).Observe(time.Since(start).Seconds())
}
