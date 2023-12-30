package main

import (
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"image/color"
	"log"
	"math"
	"time"
)

var (
	DARK_RED     = color.RGBA{R: 139, G: 0, B: 0, A: 255}     // Dark red
	LIGHT_RED    = color.RGBA{R: 255, G: 50, B: 50, A: 255}   // Light red
	DARK_YELLOW  = color.RGBA{R: 200, G: 200, B: 0, A: 255}   // Yellow
	LIGHT_YELLOW = color.RGBA{R: 255, G: 255, B: 50, A: 255}  // Lemon (lighter yellow)
	DARK_GREEN   = color.RGBA{R: 0, G: 128, B: 0, A: 255}     // Green
	LIGHT_GREEN  = color.RGBA{R: 144, G: 238, B: 144, A: 255} // Light Green
	DARK_BLUE    = color.RGBA{R: 0, G: 0, B: 255, A: 255}     // Blue
	LIGHT_BLUE   = color.RGBA{R: 135, G: 206, B: 235, A: 255} // Sky Blue (lighter blue)
	DARK_PURPLE  = color.RGBA{R: 200, G: 0, B: 200, A: 255}   // Teal
	LIGHT_PURPLE = color.RGBA{R: 255, G: 50, B: 255, A: 255}  // Light Teal

	DARK_COLORS  = []color.RGBA{DARK_RED, DARK_GREEN, DARK_BLUE, DARK_PURPLE, DARK_YELLOW}
	LIGHT_COLORS = []color.RGBA{LIGHT_RED, LIGHT_GREEN, LIGHT_BLUE, LIGHT_PURPLE, LIGHT_YELLOW}
)

type Plotter_ struct {
	m           *Metrics
	dbRequests  *plot.Plot
	allRequests *plot.Plot
}

func NewPlotter(m *Metrics) *Plotter_ {
	return &Plotter_{m: m, dbRequests: plot.New(), allRequests: plot.New()}
}

func (plt *Plotter_) PlotDatabaseRequests(fileName string) {
	start := plt.m.start
	end := plt.m.end
	p := plt.dbRequests
	p.Title.Text = "Database Requests per Second as a Function of Time"
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Requests per second"
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0

	// Define the resolution and calculate timeSlice
	resolution := int(math.Round(plt.m.config.maxDuration.Seconds()))
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))

	// Aggregate metrics into buckets based on the timeSlice
	requestCountsPerSlice := make(map[int64]int)
	metrics := plt.m.GetDatabaseRequests()
	for _, metric := range metrics {
		bucket := int64(math.Floor(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
		requestCountsPerSlice[bucket]++
	}

	// Create a plotter.XYs to hold the request counts
	pts := make(plotter.XYs, resolution)
	maxReqPerSecond := 0.0

	// Fill the pts with the request counts
	for i := 0; i < resolution; i++ {
		if count, ok := requestCountsPerSlice[int64(i)]; ok {
			reqPerSecond := float64(count) / timeSlice.Seconds()
			maxReqPerSecond = math.Max(maxReqPerSecond, reqPerSecond)
			pts[i].Y = reqPerSecond
		}
		pts[i].X = float64(i) * timeSlice.Seconds()
	}
	p.Y.Max = maxReqPerSecond * 1.2

	// Create a line chart
	line, err := plotter.NewLine(pts)
	if err != nil {
		log.Panic(err)
	}

	p.Add(line)

	nodeFailures := plt.m.GetFailureIntervals()

	for i := 0; i < len(nodeFailures); i++ {
		node := nodeFailures[i]
		for j := 0; j < len(node); j++ {
			interval := node[j]
			iStart := interval.start.Sub(start).Seconds()
			iEnd := interval.end.Sub(start).Seconds()
			addVerticalLine(p, iStart, fmt.Sprintf("node%d failed", i), DARK_COLORS[i])
			addVerticalLine(p, iEnd, fmt.Sprintf("node%d recovered", i), LIGHT_COLORS[i])
			fmt.Printf("node%d failed from %d to %d\n", i+1, int(math.Round(iStart)), int(math.Round(iEnd)))
		}
	}

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *Plotter_) PlotAllRequests(fileName string) {
	start := plt.m.start
	end := plt.m.end
	p := plt.allRequests
	p.Title.Text = "Workload (User Requests per Second) As a Function of Time"
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Requests per second"
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0

	// Define the resolution and calculate timeSlice
	resolution := int(math.Round(plt.m.config.maxDuration.Seconds()))
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))

	// Aggregate metrics into buckets based on the timeSlice
	requestCountsPerSlice := make(map[int64]int)
	metrics := plt.m.GetAllRequests()
	for _, metric := range metrics {
		bucket := int64(math.Floor(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
		requestCountsPerSlice[bucket]++
	}

	// Create a plotter.XYs to hold the request counts
	pts := make(plotter.XYs, resolution)
	maxReqPerSecond := 0.0
	sumReqPerSecond := 0.0
	countSecs := 0

	// Fill the pts with the request counts
	for i := 0; i < resolution; i++ {
		if count, ok := requestCountsPerSlice[int64(i)]; ok {
			reqPerSecond := float64(count) / timeSlice.Seconds()
			maxReqPerSecond = math.Max(maxReqPerSecond, reqPerSecond)
			sumReqPerSecond += reqPerSecond
			countSecs++
			pts[i].Y = reqPerSecond
		}
		pts[i].X = float64(i) * timeSlice.Seconds()
	}
	p.Y.Max = maxReqPerSecond * 1.2

	// Create a line chart
	line, err := plotter.NewLine(pts)
	if err != nil {
		log.Panic(err)
	}

	p.Add(line)

	mean := sumReqPerSecond / float64(countSecs)
	addHorizontalLine(p, mean, "mean requests per second", DARK_BLUE)

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func addVerticalLine(p *plot.Plot, xValue float64, label string, clr color.RGBA) {
	verticalLine, err := plotter.NewLine(plotter.XYs{{X: xValue, Y: p.Y.Min}, {X: xValue, Y: p.Y.Max}})
	if err != nil {
		panic(err)
	}
	verticalLine.Color = clr
	verticalLine.Dashes = []vg.Length{vg.Points(5), vg.Points(5)} // Dashed line

	// Add the vertical line to the plot
	p.Add(verticalLine)

	// Add a legend for the line
	p.Legend.Add(label, verticalLine)
}

func addHorizontalLine(p *plot.Plot, yValue float64, label string, clr color.RGBA) {
	verticalLine, err := plotter.NewLine(plotter.XYs{{X: p.X.Min, Y: yValue}, {X: p.X.Max, Y: yValue}})
	if err != nil {
		panic(err)
	}
	verticalLine.Color = clr
	verticalLine.Dashes = []vg.Length{vg.Points(5), vg.Points(5)} // Dashed line

	// Add the vertical line to the plot
	p.Add(verticalLine)

	// Add a legend for the line
	p.Legend.Add(label, verticalLine)
}
