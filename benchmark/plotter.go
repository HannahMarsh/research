package main

import (
	"fmt"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/text"
	"gonum.org/v1/plot/vg"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"log"
	"math"
	"os"
	"time"
)

var (
	DARK_RED     = color.RGBA{R: 139, G: 0, B: 0, A: 255}     // Dark red
	LIGHT_RED    = color.RGBA{R: 255, G: 50, B: 50, A: 255}   // Light red
	DARK_PINK    = color.RGBA{R: 200, G: 40, B: 75, A: 255}   // Dark red
	LIGHT_PINK   = color.RGBA{R: 255, G: 50, B: 150, A: 255}  // Light red
	DARK_YELLOW  = color.RGBA{R: 200, G: 200, B: 0, A: 255}   // Yellow
	LIGHT_YELLOW = color.RGBA{R: 240, G: 240, B: 0, A: 255}   // Lemon (lighter yellow)
	DARK_GREEN   = color.RGBA{R: 0, G: 128, B: 0, A: 255}     // Green
	LIGHT_GREEN  = color.RGBA{R: 20, G: 200, B: 50, A: 255}   // Light Green
	DARK_BLUE    = color.RGBA{R: 0, G: 0, B: 255, A: 255}     // Blue
	LIGHT_BLUE   = color.RGBA{R: 100, G: 170, B: 235, A: 255} // Sky Blue (lighter blue)
	DARK_PURPLE  = color.RGBA{R: 170, G: 0, B: 200, A: 255}   // Teal
	LIGHT_PURPLE = color.RGBA{R: 180, G: 50, B: 200, A: 255}  // Light Teal
	GREY         = color.RGBA{R: 120, G: 120, B: 150, A: 255} // Light Teal

	DARK_COLORS  = []color.RGBA{DARK_PINK, DARK_PURPLE, DARK_BLUE, DARK_GREEN, DARK_YELLOW, DARK_RED}
	LIGHT_COLORS = []color.RGBA{LIGHT_PINK, LIGHT_PURPLE, LIGHT_BLUE, LIGHT_GREEN, LIGHT_YELLOW, LIGHT_RED}
)

type Plotter_ struct {
	m           *Metrics
	dbRequests  *plot.Plot
	allRequests *plot.Plot
	cacheHits   *plot.Plot
	latency     *plot.Plot
	keyspace    *plot.Plot
}

func NewPlotter(m *Metrics) *Plotter_ {
	return &Plotter_{m: m, dbRequests: plot.New(), allRequests: plot.New(), cacheHits: plot.New(), latency: plot.New(), keyspace: plot.New()}
}

func (plt *Plotter_) MakePlots() {
	var path = "metrics/"
	var dbRequests = path + "requests_per_second.png"
	var allRequests = path + "all_requests_per_second.png"
	var cacheHits = path + "cache_hit_ratio.png"
	var latency = path + "latency.png"
	var keyspace = path + "keyspace.png"
	var tiled = path + "tiled.png"
	plt.PlotDatabaseRequests(dbRequests)
	plt.PlotAllRequests(allRequests)
	plt.PlotCacheHits(cacheHits)
	plt.PlotLatency(latency)
	plt.PlotKeyspacePopularities(keyspace)
	plt.TilePlots(tiled, dbRequests, allRequests, cacheHits, latency, keyspace)
}

func (plt *Plotter_) TilePlots(tiled string, fileName1 string, fileName2 string, fileName3 string, fileName4 string, fileName5 string) {
	// Open the image files.
	img1, err := openImage(fileName1)
	if err != nil {
		log.Fatal(err)
	}
	img2, err := openImage(fileName2)
	if err != nil {
		log.Fatal(err)
	}
	img3, err := openImage(fileName3)
	if err != nil {
		log.Fatal(err)
	}
	img4, err := openImage(fileName4)
	if err != nil {
		log.Fatal(err)
	}
	img5, err := openImage(fileName5)
	if err != nil {
		log.Fatal(err)
	}

	// Assuming all images are the same size.
	imgWidth := img1.Bounds().Dx()
	imgHeight := img1.Bounds().Dy()

	// Define padding between images.
	padding := 20 // pixels

	// Create a new blank image with twice the width and height of the original images plus padding.
	tiledImg := image.NewRGBA(image.Rect(0, 0, imgWidth*2+padding*3, imgHeight*2+padding*3))

	// Set the background to white.
	white := color.RGBA{R: 255, G: 255, B: 255, A: 255}
	draw.Draw(tiledImg, tiledImg.Bounds(), &image.Uniform{C: white}, image.Point{}, draw.Src)

	// Calculate the starting points for each image considering the padding.
	startPoints := []image.Point{
		{X: padding, Y: padding},                            // Image 1 starting point
		{X: imgWidth + padding*2, Y: padding},               // Image 2 starting point
		{X: padding, Y: imgHeight + padding*2},              // Image 3 starting point
		{X: imgWidth + padding*2, Y: imgHeight + padding*2}, // Image 4 starting point
	}

	// Draw the images onto the tiled image with padding.
	for i, img := range []image.Image{img1, img2, img3, img4} {
		sp := startPoints[i]
		rect := image.Rect(sp.X, sp.Y, sp.X+imgWidth, sp.Y+imgHeight)
		draw.Draw(tiledImg, rect, img, image.Point{}, draw.Src)
	}

	// Save the tiled image to a new PNG file.
	outFile, err := os.Create(tiled)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()
	err = png.Encode(outFile, tiledImg)
	if err != nil {
		panic(err)
	}
}

// openImage is a helper function to open and decode an image file.
func openImage(filename string) (image.Image, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)
	img, _, err := image.Decode(file)
	return img, err
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
		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
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

	plt.plotNodeFailures(p)

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *Plotter_) getPoints(metrics []Metric, filter func(Metric) bool) (plotter.XYs, float64, float64) {
	// Define the resolution and calculate timeSlice
	resolution := int(math.Round(plt.m.config.maxDuration.Seconds()))
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))

	countsPerSlice := make(map[int64]int)
	for _, metric := range metrics {
		if filter(metric) {
			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
			countsPerSlice[bucket]++
		}
	}
	pts := make(plotter.XYs, resolution)
	count_ := 0.0
	sum := 0.0
	maxCountPerSecond := 0.0

	// Fill the pts with the request counts
	for i := 0; i < resolution; i++ {
		if count, ok := countsPerSlice[int64(i)]; ok {
			countPerSecond := float64(count) / timeSlice.Seconds()
			maxCountPerSecond = math.Max(maxCountPerSecond, countPerSecond)
			pts[i].Y = countPerSecond
			sum += countPerSecond
			count_++
		}
		pts[i].X = float64(i) * timeSlice.Seconds()
	}
	if count_ > 0 {
		return pts, maxCountPerSecond, sum / count_
	}
	return pts, maxCountPerSecond, 0
}

func (plt *Plotter_) PlotKeyspacePopularities(fileName string) {
	start := plt.m.start
	end := plt.m.end
	p := plt.keyspace
	p.Title.Text = "Keyspace Popularity as a Function of Time"
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

	metrics := plt.m.GetKeyspacePopularities()
	for i := 0; i < len(plt.m.config.keyspacePop); i++ {
		pts, maxCount, mean := plt.getPoints(metrics, func(metric Metric) bool {
			return int(math.Round(metric.floatValues["keyspace"])) == i
		})
		p.Y.Max = math.Max(p.Y.Max, maxCount*1.2)
		line, err := plotter.NewLine(pts)
		if err != nil {
			log.Panic(err)
		}
		line.Color = DARK_COLORS[i]
		p.Add(line)
		p.Legend.Add(fmt.Sprintf("keyspace %d", i), line)
		addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), LIGHT_COLORS[i])
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
	p.Title.Text = "User Requests per Second As a Function of Time"
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
	unsuccessfulRequestCountsPerSlice := make(map[int64]int)
	successfulRequestCountsPerSlice := make(map[int64]int)
	metrics := plt.m.GetAllRequests()
	for _, metric := range metrics {
		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
		if metric.stringValues["successful"] == "true" {
			successfulRequestCountsPerSlice[bucket]++
		} else {
			unsuccessfulRequestCountsPerSlice[bucket]++
		}
	}

	// Create a plotter.XYs to hold the request counts
	unsuccessfulPts := make(plotter.XYs, resolution)
	successfulPts := make(plotter.XYs, resolution)
	maxReqPerSecond := 0.0
	sumReqPerSecond := 0.0
	countSecs := 0

	// Fill the unsuccessfulPts with the request counts
	for i := 0; i < resolution; i++ {
		if count, ok := unsuccessfulRequestCountsPerSlice[int64(i)]; ok {
			reqPerSecond := float64(count) / timeSlice.Seconds()
			maxReqPerSecond = math.Max(maxReqPerSecond, reqPerSecond)
			sumReqPerSecond += reqPerSecond
			countSecs++
			unsuccessfulPts[i].Y = reqPerSecond
		}
		if count, ok := successfulRequestCountsPerSlice[int64(i)]; ok {
			reqPerSecond := float64(count) / timeSlice.Seconds()
			maxReqPerSecond = math.Max(maxReqPerSecond, reqPerSecond)
			sumReqPerSecond += reqPerSecond
			countSecs++
			successfulPts[i].Y = reqPerSecond
		}
		unsuccessfulPts[i].X = float64(i) * timeSlice.Seconds()
		successfulPts[i].X = float64(i) * timeSlice.Seconds()
	}
	p.Y.Max = maxReqPerSecond * 1.2

	line2, err := plotter.NewLine(successfulPts)
	if err != nil {
		log.Panic(err)
	}
	line2.Color = DARK_GREEN
	p.Legend.Add("successful", line2)
	p.Add(line2)

	line, err := plotter.NewLine(unsuccessfulPts)
	if err != nil {
		log.Panic(err)
	}
	line.Color = DARK_RED
	p.Legend.Add("unsuccessful", line)
	p.Add(line)

	//mean := sumReqPerSecond / float64(countSecs)
	//addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), GREY)

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *Plotter_) PlotLatency(fileName string) {
	start := plt.m.start
	end := plt.m.end
	p := plt.latency
	p.Title.Text = "Request Latency As a Function of Time"
	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Average Latency (ms)"
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0

	// Adjust legend position
	p.Legend.Top = true            // Position the legend at the top of the plot
	p.Legend.Left = true           // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
	p.Legend.YOffs = vg.Points(30) // Move the legend up

	// Define the resolution and calculate timeSlice
	resolution := 35
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))

	// Aggregate metrics into buckets based on the timeSlice
	totalLatencyPerSlice := make(map[int64]float64)
	countPerSlice := make(map[int64]int)
	metrics := plt.m.GetLatency()
	for _, metric := range metrics {
		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Seconds() / float64(timeSlice.Seconds()))))
		totalLatencyPerSlice[bucket] += metric.floatValues["latency"]
		countPerSlice[bucket]++
	}

	averageLatencyPerSlice := make(map[int64]float64)

	for i := 0; i < resolution; i++ {
		if countPerSlice[int64(i)] > 0 {
			averageLatencyPerSlice[int64(i)] = 1000 * totalLatencyPerSlice[int64(i)] / float64(countPerSlice[int64(i)])
		} else {
			averageLatencyPerSlice[int64(i)] = 0.0
		}
	}

	// Create a plotter.XYs to hold the request counts
	pts := make(plotter.XYs, resolution)
	maxLatency := 0.0
	sumLatency := 0.0
	countLatency := 0

	// Fill the pts with the request counts
	for i := 0; i < resolution; i++ {
		if latency, ok := averageLatencyPerSlice[int64(i)]; ok {
			maxLatency = math.Max(maxLatency, latency)
			sumLatency += latency
			countLatency++
			pts[i].Y = latency
		} else {
			pts[i].Y = 0.0
		}
		pts[i].X = float64(i) * timeSlice.Seconds()
	}
	p.Y.Max = maxLatency * 1.2

	// Create a line chart
	line, err := plotter.NewLine(pts)
	if err != nil {
		log.Panic(err)
	}

	p.Add(line)

	mean := sumLatency / float64(countLatency)
	addHorizontalLine(p, mean, fmt.Sprintf(" mean\n (%.2f ms)", mean), GREY)

	plt.plotNodeFailures(p)

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
		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
		cacheHitsPerSlice[bucket]++
	}
	for _, metric := range requests {
		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
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

	plt.plotNodeFailures(p)

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *Plotter_) plotNodeFailures(p *plot.Plot) {

	nodeFailures := plt.m.GetFailureIntervals()

	for i := 0; i < len(nodeFailures); i++ {
		metric := nodeFailures[i]
		iStart := metric.timestamp
		duration := time.Duration(metric.floatValues["duration"] * float64(time.Second))
		iEnd := iStart.Add(duration)
		addVerticalLine(p, iStart.Sub(plt.m.start).Seconds(), fmt.Sprintf("node%d\nfailed", i+1), LIGHT_COLORS[i])
		addVerticalLine(p, iEnd.Sub(plt.m.start).Seconds(), fmt.Sprintf("node%d\nrecovered", i+1), LIGHT_COLORS[i])
		//fmt.Printf("node%d\nfailed from %d to %d\n", i+1, int(math.Round(iStart.Sub(plt.m.start).Seconds())), int(math.Round(iEnd.Sub(plt.m.start).Seconds())))
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
		if int(metric.floatValues["nodeIndex"]) == nodeIndex {
			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
			cacheHitsPerSlice[bucket]++
		}
	}
	for _, metric := range requests {
		if int(metric.floatValues["nodeIndex"]) == nodeIndex {
			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
			requestsPerSlice[bucket]++
		}
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
	labels.TextStyle[0].Font.Size = 11
	labels.TextStyle[0].Rotation = 0.9

	p.Add(verticalLine)
	p.Add(labels)
}

func addHorizontalLine(p *plot.Plot, yValue float64, label string, clr color.RGBA) {
	horizontalLine, err := plotter.NewLine(plotter.XYs{{X: p.X.Min, Y: yValue}, {X: p.X.Max, Y: yValue}})
	if err != nil {
		panic(err)
	}
	horizontalLine.Color = clr
	horizontalLine.Dashes = []vg.Length{vg.Points(5), vg.Points(5)} // Dashed line

	// Add a legend for the line
	labels, _ := plotter.NewLabels(plotter.XYLabels{
		XYs: []plotter.XY{
			{X: p.X.Max, Y: yValue},
		},
		Labels: []string{label},
	})
	labels.TextStyle[0].Color = clr           // Set the label color
	labels.TextStyle[0].YAlign = text.YCenter // Align the label above the line
	labels.TextStyle[0].XAlign = text.XLeft   // Align the label right of the line
	labels.Offset = vg.Point{X: 3, Y: 0}      // Adjust the X offset to move label closer to the line

	p.Add(horizontalLine)
	p.Add(labels)
}
