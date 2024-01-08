package main

import (
	"fmt"
	"github.com/golang/freetype"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/text"
	"gonum.org/v1/plot/vg"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io/ioutil"
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
	m            *Metrics
	dbRequests   *plot.Plot
	allRequests  *plot.Plot
	cacheHits    *plot.Plot
	latency      *plot.Plot
	keyspace     *plot.Plot
	nodeRequests *plot.Plot
	cacheSizes   *plot.Plot
	end          time.Time
	start        time.Time
}

func NewPlotter(m *Metrics) *Plotter_ {
	plt := &Plotter_{m: m, dbRequests: plot.New(), allRequests: plot.New(), cacheHits: plot.New(), latency: plot.New(), keyspace: plot.New(), nodeRequests: plot.New(), cacheSizes: plot.New(), end: m.end, start: m.start}
	//plt.initPlots()
	return plt
}

func (plt *Plotter_) initPlots() {
	plt.initPlot(plt.dbRequests, "Database Requests per Second as a Function of Time", "Requests per second")
	plt.initPlot(plt.allRequests, "Total Requests per Second As a Function of Time", "Requests per second")
	plt.initPlot(plt.cacheHits, "Cache Hit Ratio as a Function of Time", "Cache Hit Ratio")
	plt.initPlot(plt.latency, "Request Latency As a Function of Time", "Average Latency (ms)")
	plt.initPlot(plt.keyspace, "Keyspace Popularity as a Function of Time", "Requests per second")
	plt.initPlot(plt.nodeRequests, "Cache Requests as a Function of Time", "Requests per second")
	plt.initPlot(plt.cacheSizes, "Cache Sizes as a Function of Time", "Number of Items")
}

func (plt *Plotter_) initPlot(p *plot.Plot, title string, yAxis string) {
	start := plt.m.start
	end := plt.m.end
	p.Title.Text = title
	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = yAxis
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0

	// Adjust legend position
	p.Legend.Top = true            // Position the legend at the top of the plot
	p.Legend.Left = true           // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
	p.Legend.YOffs = vg.Points(30) // Move the legend up
}

func (plt *Plotter_) MakePlots() {
	plt.MakePlotsFrom(plt.m.start, plt.m.end)
}

func (plt *Plotter_) MakePlotsFrom(start time.Time, end time.Time) {
	plt.end = end
	plt.start = start
	var path = plt.m.config.metricsPath + "/individual/"
	var dbRequests = path + "requests_per_second.png"
	var allRequests = path + "all_requests_per_second.png"
	var cacheHits = path + "cache_hit_ratio.png"
	var latency = path + "latency.png"
	var keyspace = path + "keyspace.png"
	var nodes = path + "nodes.png"
	var cacheSize = path + "cacheSizes.png"
	var tiled = path + "../metrics.png"
	var config = path + "config.png"
	plt.PlotDatabaseRequests(dbRequests)
	plt.PlotAllRequests(allRequests)
	plt.PlotCacheHits(cacheHits)
	plt.PlotLatency(latency)
	plt.PlotKeyspacePopularities(keyspace)
	plt.PlotConfig(config)
	plt.PlotNodes(nodes)
	plt.PlotCacheSizes(cacheSize)
	plt.TilePlots(tiled, [][]string{
		{config, allRequests},
		{keyspace, dbRequests},
		{latency, cacheHits},
		{nodes, cacheSize},
	})
}

func (plt *Plotter_) TilePlots(tiled string, fileNames [][]string) {
	// Define padding between images.
	padding := 20 // pixels

	// Load all the images and find out the max width and height.
	// Store nil for blanks to be filled in later.
	imgs := make([][]image.Image, len(fileNames))
	var maxWidth, maxHeight, maxRowLength int
	for i, row := range fileNames {
		imgs[i] = make([]image.Image, len(row))
		if len(row) > maxRowLength {
			maxRowLength = len(row) // Track the max row length to handle blanks.
		}
		for j, fileName := range row {
			if fileName == "" {
				imgs[i][j] = nil // If there's no filename, we'll insert a blank later.
				continue
			}
			img, err := openImage(fileName)
			if err != nil {
				log.Fatal(err)
			}
			imgs[i][j] = img
			if img.Bounds().Dx() > maxWidth {
				maxWidth = img.Bounds().Dx()
			}
			if img.Bounds().Dy() > maxHeight {
				maxHeight = img.Bounds().Dy()
			}
		}
	}

	// Create a new blank image with the total width and height based on the images and padding.
	tiledImg := image.NewRGBA(image.Rect(0, 0, maxWidth*maxRowLength+padding*(maxRowLength+1), maxHeight*len(fileNames)+padding*(len(fileNames)+1)))

	// Set the background to white.
	white := color.RGBA{R: 255, G: 255, B: 255, A: 255}
	draw.Draw(tiledImg, tiledImg.Bounds(), &image.Uniform{C: white}, image.Point{}, draw.Src)

	// Draw the images onto the tiled image.
	for i, row := range imgs {
		for j := 0; j < maxRowLength; j++ { // Iterate up to the maximum row length.
			sp := image.Point{X: j*maxWidth + (j+1)*padding, Y: i*maxHeight + (i+1)*padding}
			rect := image.Rect(sp.X, sp.Y, sp.X+maxWidth, sp.Y+maxHeight)
			if j < len(row) && row[j] != nil {
				// Draw the actual image if it exists.
				draw.Draw(tiledImg, rect, row[j], image.Point{}, draw.Src)
			} else {
				// Fill in a blank rectangle otherwise.
				draw.Draw(tiledImg, rect, &image.Uniform{C: white}, image.Point{}, draw.Src)
			}
		}
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

func addLabel(img *image.RGBA, x, y int, label string, fontPath string, size float64) {
	// Read the font data.
	fontBytes, err := ioutil.ReadFile(fontPath)
	if err != nil {
		log.Println(err)
		return
	}
	f, err := freetype.ParseFont(fontBytes)
	if err != nil {
		log.Println(err)
		return
	}

	// Initialize the context.
	c := freetype.NewContext()
	c.SetDPI(72)
	c.SetFont(f)
	c.SetFontSize(size)
	c.SetClip(img.Bounds())
	c.SetDst(img)
	c.SetSrc(image.NewUniform(color.Black))

	// Calculate the point.
	pt := freetype.Pt(x, y+int(c.PointToFixed(size)>>6)) // The Y-coordinate is the baseline.

	// Draw the text.
	_, err = c.DrawString(label, pt)
	if err != nil {
		log.Println(err)
	}
}

func (plt *Plotter_) PlotConfig(filename string) {
	config := plt.m.config
	// Create a blank image with enough space
	img := image.NewRGBA(image.Rect(0, 0, 800, 400))

	// Fill the image with a background color (white)
	draw.Draw(img, img.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)

	leftIndent := 50

	// Draw the txt on the image
	addLabel(img, leftIndent+20, 40, "Configuration Summary:", "fonts/roboto/Roboto-Bold.ttf", 18.0)
	addLabel(img, leftIndent+40, 90, fmt.Sprintf("Duration: %d seconds", int(config.maxDuration.Seconds())), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 120, fmt.Sprintf("Num Requests: %d", config.numRequests), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 150, fmt.Sprintf("Nodes: %d", len(config.nodeConfigs)), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 180, fmt.Sprintf("Virtual Nodes: %d", config.virtualNodes), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 210, fmt.Sprintf("Read Percentage: %d%%", int(config.readPercentage*100)), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 240, fmt.Sprintf("Failures: %d", len(config.failures)), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 270, fmt.Sprintf("Num Possible Keys: %d", config.numPossibleKeys), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 300, fmt.Sprintf("Keyspace Weights: %v", config.keyspacePop), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 330, fmt.Sprintf("Cache Default Expiration: %d", config.cacheExpiration), "fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 360, fmt.Sprintf("Cache Cleanup Interval: %d", config.cacheCleanupInterval), "fonts/roboto/Roboto-Medium.ttf", 16.0)

	// Save the image to file
	saveImage(filename, img)

}

func saveImage(filename string, img *image.RGBA) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer func(f *os.File) {
		if err = f.Close(); err != nil {
			log.Fatalf("Failed to close file %s: %v", filename, err)
		}
	}(f)

	if err := png.Encode(f, img); err != nil {
		log.Fatalf("Failed to encode image: %v", err)
	}
}

func (plt *Plotter_) getCountsPerTimeSlice(metrics []Metric, filter func(Metric) bool) (plotter.XYs, float64, float64) {
	// Define the resolution and calculate timeSlice
	resolution := 30
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))

	countsPerSlice := make(map[int64]int)
	for _, metric := range metrics {
		if filter(metric) {
			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(plt.m.start).Microseconds()) / float64(timeSlice.Microseconds())))
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

func (plt *Plotter_) getAveragePerTimeSlice(metrics []Metric, filter func(Metric) bool) (plotter.XYs, float64, float64) {
	// Define the resolution and calculate timeSlice
	resolution := 30
	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
	sumPerSlice := make(map[int64]int64)
	countsPerSlice := make(map[int64]int)
	averagePerSlice := make(map[int64]float64)

	for _, metric := range metrics {
		if filter(metric) {
			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(plt.m.start).Microseconds()) / float64(timeSlice.Microseconds())))
			countsPerSlice[bucket]++
			sumPerSlice[bucket] += int64(metric.floatValues["size"])
			averagePerSlice[bucket] = float64(sumPerSlice[bucket]) / float64(countsPerSlice[bucket])
		}
	}
	pts := make(plotter.XYs, resolution)
	count_ := 0.0
	sum := 0.0
	maxCountPerSecond := 0.0

	// Fill the pts with the request counts
	for i := 0; i < resolution; i++ {
		if average, ok := averagePerSlice[int64(i)]; ok {
			maxCountPerSecond = math.Max(maxCountPerSecond, average)
			pts[i].Y = average
			sum += average
			count_++
		}
		pts[i].X = float64(i) * timeSlice.Seconds()
	}
	if count_ > 0 {
		return pts, maxCountPerSecond, sum / count_
	}
	return pts, maxCountPerSecond, 0
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
	resolution := 30
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

func (plt *Plotter_) PlotKeyspacePopularities(fileName string) {
	start := plt.m.start
	end := plt.end
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
		pts, maxCount, mean := plt.getCountsPerTimeSlice(metrics, func(metric Metric) bool {
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

func (plt *Plotter_) PlotNodes(fileName string) {
	start := plt.m.start
	end := plt.end
	p := plt.nodeRequests
	p.Title.Text = "Cache Requests as a Function of Time"
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

	metrics := plt.m.GetAllRequests()
	for i := 0; i < len(plt.m.config.nodeConfigs); i++ {
		pts, maxCount, mean := plt.getCountsPerTimeSlice(metrics, func(metric Metric) bool {
			return int(math.Round(metric.floatValues["nodeIndex"])) == i
		})
		p.Y.Max = math.Max(p.Y.Max, maxCount*1.2)
		line, err := plotter.NewLine(pts)
		if err != nil {
			log.Panic(err)
		}
		line.Color = DARK_COLORS[i]
		p.Add(line)
		p.Legend.Add(fmt.Sprintf("node%d", i+1), line)
		addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), LIGHT_COLORS[i])
	}

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *Plotter_) PlotCacheSizes(fileName string) {
	start := plt.m.start
	end := plt.end
	p := plt.cacheSizes
	p.Title.Text = "Cache Sizes as a Function of Time"
	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Number of Items"
	p.X.Min = 0.0
	p.X.Max = end.Sub(start).Seconds()
	p.Y.Min = 0.0

	// Adjust legend position
	p.Legend.Top = true            // Position the legend at the top of the plot
	p.Legend.Left = true           // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
	p.Legend.YOffs = vg.Points(30) // Move the legend up

	metrics := plt.m.GetCacheSizes()
	for i := 0; i < len(plt.m.config.nodeConfigs); i++ {
		pts, maxCount, mean := plt.getAveragePerTimeSlice(metrics, func(metric Metric) bool {
			return int(math.Round(metric.floatValues["nodeIndex"])) == i
		})
		p.Y.Max = math.Max(p.Y.Max, maxCount*1.2)
		line, err := plotter.NewLine(pts)
		if err != nil {
			log.Panic(err)
		}
		line.Color = DARK_COLORS[i]
		p.Add(line)
		p.Legend.Add(fmt.Sprintf("node%d", i+1), line)
		addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), LIGHT_COLORS[i])
	}

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *Plotter_) PlotAllRequests(fileName string) {
	start := plt.m.start
	end := plt.end
	p := plt.allRequests
	p.Title.Text = "Total Requests per Second As a Function of Time"
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
	resolution := 30
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
	end := plt.end
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
	resolution := 30
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
	end := plt.end
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
	resolution := 30
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
	resolution := 30
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
