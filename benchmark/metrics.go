package main

import (
	"bufio"
	"embed"
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

//go:embed metrics
var metricsDir embed.FS

func PlotMetric(body io.ReadCloser, metricName string) {
	scanner := bufio.NewScanner(body)
	pts := make(plotter.XYs, 0)

	var i int
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, metricName) {
			parts := strings.Fields(line)
			if len(parts) != 2 {
				continue
			}

			value, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				log.Fatalf("Error parsing value: %v", err)
			}

			pts = append(pts, plotter.XY{X: float64(i), Y: value})
			i++
		}
	}

	p := plot.New()
	if p == nil {
		log.Fatalf("Error creating plot")
	}

	p.Title.Text = fmt.Sprintf("Plot for %s", metricName)
	p.X.Label.Text = "Index"
	p.Y.Label.Text = "Value"

	err := plotutil.AddLinePoints(p, metricName, pts)
	if err != nil {
		log.Fatalf("Error adding line points: %v", err)
	}

	if err := p.Save(6*vg.Inch, 6*vg.Inch, fmt.Sprintf("metrics/%s.png", metricName)); err != nil {
		log.Fatalf("Error saving plot: %v", err)
	}
	fmt.Printf("Plot for %s saved\n", metricName)
}

func Plot(url string) {
	// Query Prometheus for metric data.
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error querying Prometheus: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("error closing reader: %s", err)
		}
	}(resp.Body)
	metricsData, err := io.ReadAll(resp.Body)
	// Open the file in append mode. If it doesn't exist, create it with permissions.
	f, err := os.OpenFile("metrics/result.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(f)

	if _, err := f.WriteString(string(metricsData)); err != nil {
		log.Fatal(err)
	}

	PlotMetric(resp.Body, "benchmark_cache_hits_total")
}
