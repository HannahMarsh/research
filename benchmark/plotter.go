package main

import (
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/text"
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
	LIGHT_GREEN  = color.RGBA{R: 20, G: 200, B: 50, A: 255}   // Light Green
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
	cacheHits   *plot.Plot
}

func NewPlotter(m *Metrics) *Plotter_ {
	return &Plotter_{m: m, dbRequests: plot.New(), allRequests: plot.New(), cacheHits: plot.New()}
}

func (plt *Plotter_) PlotDatabaseRequests(fileName string) {
	start := plt.m.start
	end := plt.m.end
	p := plt.dbRequests
	p.Title.Text = "Database Requests per Second as a Function of Time"
	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Requests per second"
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0

	// Adjust legend position
	p.Legend.Top = true            // Position the legend at the top of the plot
	p.Legend.Left = true           // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
	p.Legend.YOffs = vg.Points(30) // Move the legend up

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
			addVerticalLine(p, iStart, fmt.Sprintf("node%d\nfailed", i+1), LIGHT_COLORS[i])
			addVerticalLine(p, iEnd, fmt.Sprintf("node%d\nrecovered", i+1), LIGHT_COLORS[i])
			fmt.Printf("node%d\nfailed from %d to %d\n", i+1, int(math.Round(iStart)), int(math.Round(iEnd)))
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
	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Requests per second"
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0

	// Adjust legend position
	p.Legend.Top = true            // Position the legend at the top of the plot
	p.Legend.Left = true           // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
	p.Legend.YOffs = vg.Points(30) // Move the legend up

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

func (plt *Plotter_) PlotCacheHits(fileName string) {
	start := plt.m.start
	end := plt.m.end
	p := plt.cacheHits
	p.Title.Text = "Cache Hit Ratio as a Function of Time"
	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	// Adjust padding around the entire plot
	p.X.Label.Padding = vg.Points(10)
	p.Y.Label.Padding = vg.Points(10)

	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Cache Hit Ratio"
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0
	p.Y.Max = 1.0

	// Adjust legend position
	p.Legend.Top = true            // Position the legend at the top of the plot
	p.Legend.Left = true           // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
	p.Legend.YOffs = vg.Points(30) // Move the legend up
	p.Legend.TextStyle.Font.Size = 11

	// Define the resolution and calculate timeSlice
	resolution := int(math.Round(plt.m.config.maxDuration.Seconds()))
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))

	// Aggregate metrics into buckets based on the timeSlice
	cacheHitsPerSlice := make(map[int64]int)
	requestsPerSlice := make(map[int64]int)
	metrics := plt.m.GetCacheHits()
	requests := plt.m.GetAllRequests()
	for _, metric := range metrics {
		bucket := int64(math.Floor(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
		cacheHitsPerSlice[bucket]++
	}
	for _, metric := range requests {
		bucket := int64(math.Floor(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
		requestsPerSlice[bucket]++
	}

	// Create a plotter.XYs to hold the request counts
	pts := make(plotter.XYs, resolution)

	// Fill the pts with the request counts
	for i := 0; i < resolution; i++ {
		if hits, ok := cacheHitsPerSlice[int64(i)]; ok {
			hitsPerSecond := float64(hits) / timeSlice.Seconds()
			if req, ok2 := requestsPerSlice[int64(i)]; ok2 {
				reqsPerSecond := float64(req) / timeSlice.Seconds()
				hitRatio := hitsPerSecond / reqsPerSecond
				pts[i].Y = hitRatio
			}
		}
		pts[i].X = float64(i) * timeSlice.Seconds()
	}

	// Create a line chart
	line, err := plotter.NewLine(pts)
	if err != nil {
		log.Panic(err)
	}

	p.Legend.Add("aggregate", line)
	p.Add(line)

	for i := 0; i < len(plt.m.config.nodeConfigs); i++ {
		plt.PlotCacheHitsForNode(p, i)
	}

	nodeFailures := plt.m.GetFailureIntervals()

	for i := 0; i < len(nodeFailures); i++ {
		node := nodeFailures[i]
		for j := 0; j < len(node); j++ {
			interval := node[j]
			iStart := interval.start.Sub(start).Seconds()
			iEnd := interval.end.Sub(start).Seconds()
			addVerticalLine(p, iStart, fmt.Sprintf("node%d\nfailed", i+1), LIGHT_COLORS[i])
			addVerticalLine(p, iEnd, fmt.Sprintf("node%d\nrecovered", i+1), LIGHT_COLORS[i])
			fmt.Printf("node%d\nfailed from %d to %d\n", i+1, int(math.Round(iStart)), int(math.Round(iEnd)))
		}
	}

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *Plotter_) PlotCacheHitsForNode(p *plot.Plot, nodeIndex int) {
	start := plt.m.start

	// Define the resolution and calculate timeSlice
	resolution := int(math.Round(plt.m.config.maxDuration.Seconds()))
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))

	// Aggregate metrics into buckets based on the timeSlice
	cacheHitsPerSlice := make(map[int64]int)
	requestsPerSlice := make(map[int64]int)
	metrics := plt.m.GetCacheHits()
	requests := plt.m.GetAllRequests()
	for _, metric := range metrics {
		if int(metric.value) == nodeIndex {
			bucket := int64(math.Floor(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
			cacheHitsPerSlice[bucket]++
		}
	}
	for _, metric := range requests {
		bucket := int64(math.Floor(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
		requestsPerSlice[bucket]++
	}

	// Create a plotter.XYs to hold the request counts
	pts := make(plotter.XYs, resolution)

	// Fill the pts with the request counts
	for i := 0; i < resolution; i++ {
		if hits, ok := cacheHitsPerSlice[int64(i)]; ok {
			hitsPerSecond := float64(hits) / timeSlice.Seconds()
			if req, ok2 := requestsPerSlice[int64(i)]; ok2 {
				reqsPerSecond := float64(req) / timeSlice.Seconds()
				hitRatio := hitsPerSecond / reqsPerSecond
				pts[i].Y = hitRatio
			}
		}
		pts[i].X = float64(i) * timeSlice.Seconds()
	}

	// Create a line chart
	line, err := plotter.NewLine(pts)
	if err != nil {
		log.Panic(err)
	}

	line.Color = DARK_COLORS[nodeIndex]
	p.Legend.Add(fmt.Sprintf("node%d", nodeIndex+1), line)

	p.Add(line)
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
	labels, _ := plotter.NewLabels(plotter.XYLabels{
		XYs: []plotter.XY{
			{X: xValue, Y: p.Y.Max},
		},
		Labels: []string{label},
	})
	labels.TextStyle[0].Color = clr           // Set the label color
	labels.TextStyle[0].YAlign = text.YBottom // Align the label above the line
	labels.TextStyle[0].XAlign = text.XCenter // Align the label above the line

	p.Add(labels, verticalLine)
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
