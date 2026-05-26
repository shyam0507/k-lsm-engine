package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shyam0507/k-lsm-engine/internal/storage"
)

var requestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "klsm",
		Name:      "http_request_duration_seconds",
		Help:      "Request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	},
	[]string{"method", "path"},
)

func init() {
	prometheus.MustRegister(requestDuration)
}

func main() {
	e := storage.NewEngine()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		timer := prometheus.NewTimer(requestDuration.WithLabelValues(r.Method, "/key"))
		defer timer.ObserveDuration()

		method := r.Method
		path := r.URL.Path

		switch method {
		case "GET":
			val, ok := e.Get(path)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprint(w, val)

		case "PUT":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}

			e.Put(path, string(body))

			// flush to ss table
			storage.SaveSSTable(e.GetAll(), "sst-1.json")
			fmt.Fprintf(w, "%s", body)

		case "DELETE":
			e.Delete(path)

		default:
			w.Header().Set("Allow", "GET, PUT, DELETE")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("error while running the server: ", err)
	}
}
