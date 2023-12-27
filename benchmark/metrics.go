package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

// PrometheusResponse represents a response from Prometheus API.
type PrometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"`
		} `json:"result"`
	} `json:"data"`
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read metrics response: %v", err)
	}

	// Parse the JSON response.
	var promResp PrometheusResponse
	err = json.Unmarshal(body, &promResp)
	if err != nil {
		log.Fatalf("Error unmarshaling response: %v", err)
	}

	// Create a new plot.
	p := plot.New()
	if p == nil {
		log.Fatalf("Error creating plot")
	}

	p.Title.Text = "My Prometheus Metric Plot"
	p.X.Label.Text = "Time"
	p.Y.Label.Text = "Value"

	// Prepare the data for plotting.
	for _, result := range promResp.Data.Result {
		pts := make(plotter.XYs, len(result.Values))
		for i, v := range result.Values {
			timestamp := v[0].(float64)
			value := v[1].(string)

			// Convert the timestamp to a time.Time object and the value to a float64.
			t := time.Unix(int64(timestamp), 0)
			val, err := strconv.ParseFloat(value, 64)
			if err != nil {
				log.Fatalf("Error parsing value: %v", err)
			}

			pts[i].X = float64(t.Unix())
			pts[i].Y = val
		}

		// Add the data to the plot.
		err = plotutil.AddLinePoints(p, result.Metric["__name__"], pts)
		if err != nil {
			log.Fatalf("Error adding line points: %v", err)
		}
	}

	// Save the plot to a PNG file.
	if err := p.Save(8*vg.Inch, 4*vg.Inch, "metric_plot.png"); err != nil {
		log.Fatalf("Error saving plot: %v", err)
	}
	fmt.Println("Plot saved to 'metric_plot.png'")
}
