package metrics

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

type plotInfo struct {
	categories []category
	numBuckets int
	title      string
	yAxis      string
	path       string
	start      time.Time
	end        time.Time
}

func getStringValue(a interface{}) (string, bool) {
	if val, ok := a.(string); ok {
		return val, true
	}
	return "", false
}

func getFloatValue(a interface{}) (float64, bool) {
	if val, ok := a.(float64); ok {
		return val, true
	}
	return -1.0, false
}

var STRING = "string"
var FLOAT = "float"

type category struct {
	filter    func(Metric) bool
	plotLabel string
	color     color.RGBA
}

func equal(a interface{}, b interface{}) bool {
	switch val := a.(type) {
	case string:
		if val2, ok := b.(string); ok {
			return val == val2
		}
	case int:
		if val2, ok := b.(int); ok {
			return val == val2
		}
	case int64:
		if val2, ok := b.(int64); ok {
			return val == val2
		}
	case int32:
		if val2, ok := b.(int32); ok {
			return val == val2
		}
	case float64:
		if val2, ok := b.(float64); ok {
			return val == val2
		}
	case bool:
		if val2, ok := b.(bool); ok {
			return val == val2
		}
	}
	return false
}

func has(m Metric, label string, value interface{}) bool {
	if val, exists := m.tags[label]; exists {
		return equal(val, value)
	}
	return false
}

func PlotMetrics(start time.Time, end time.Time, path string) {
	GatherAllMetrics()
	numBuckets := 30
	var pi = []*plotInfo{
		{
			title: "Database Requests per Second as a Function of Time",
			yAxis: "Requests per second",
			categories: []category{
				{
					filter: func(m Metric) bool {
						return m.metricType == DATABASE_OPERATION && has(m, SUCCESSFUL, true)
					},
					plotLabel: "Successful",
					color:     DARK_GREEN,
				},
				{
					filter: func(m Metric) bool {
						return m.metricType == DATABASE_OPERATION && has(m, SUCCESSFUL, false)
					},
					plotLabel: "Unsuccessful",
					color:     DARK_RED,
				},
			},
			start:      start,
			end:        end,
			path:       path + "requests_per_second.png",
			numBuckets: numBuckets,
		},
		{
			title: "Cache Requests as a Function of Time",
			yAxis: "Requests per second",
			categories: []category{
				{
					filter: func(m Metric) bool {
						return m.metricType == CACHE_OPERATION && has(m, SUCCESSFUL, true)
					},
					plotLabel: "Successful",
					color:     DARK_GREEN,
				},
				{
					filter: func(m Metric) bool {
						return m.metricType == CACHE_OPERATION && has(m, SUCCESSFUL, false)
					},
					plotLabel: "Unsuccessful",
					color:     DARK_RED,
				},
			},
			start:      start,
			end:        end,
			path:       path + "cache_requests.png",
			numBuckets: numBuckets,
		},
		//initPlotInfo(start, end, GetMetricsByType(DATABASE_OPERATION), "Database Requests per Second as a Function of Time", "Requests per second", path+"requests_per_second.png"),
		//initPlotInfo(start, end, GetMetricsByType(CACHE_OPERATION), "Cache Requests as a Function of Time", "Requests per second", path+"cache_requests.png"),
		//initPlotInfo(start, end, "Total Requests per Second As a Function of Time", "Requests per second", path+"all_requests_per_second.png"),
		//initPlotInfo(start, end, "Cache Hit Ratio as a Function of Time", "Cache Hit Ratio", path+"cache_hit_ratio.png"),
		//initPlotInfo(start, end, "Request Latency As a Function of Time", "Average Latency (ms)", path+"latency.png"),
		//initPlotInfo(start, end, "Keyspace Popularity as a Function of Time", "Requests per second", path+"keyspace.png"),
		//initPlotInfo(start, end, "Cache Requests as a Function of Time", "Requests per second", path+"cache_requests.png"),
		//initPlotInfo(start, end, "Cache Sizes as a Function of Time", "Number of Items", path+"cacheSizes.png"),
	}
	cols := 2

	rows := int(math.Ceil(float64(len(pi)+1) / float64(cols)))
	var piPath [][]string
	curIndex := 0
	for r := 0; r < rows; r++ {
		var row []string
		for c := 0; c < cols; c++ {
			if r == 0 && c == 0 {
				plotConfig(start, end, path+"config.png")
				row = append(row, path+"config.png")
			} else {
				if curIndex < len(pi) {
					pi[curIndex].makePlot()
					row = append(row, pi[curIndex].path)
					curIndex += 1
				}
			}

		}
		piPath = append(piPath, row)
	}

	tilePlots(path+"tiled.png", piPath)

	//dbRequests := initPlotInfo(start, end, "Database Requests per Second as a Function of Time", "Requests per second", path+"requests_per_second.png")
	//allRequests := initPlotInfo(start, end, "Total Requests per Second As a Function of Time", "Requests per second", path+"all_requests_per_second.png")
	//cacheHits := initPlotInfo(start, end, "Cache Hit Ratio as a Function of Time", "Cache Hit Ratio", path+"cache_hit_ratio.png")
	//latency := initPlotInfo(start, end, "Request Latency As a Function of Time", "Average Latency (ms)", path+"latency.png")
	//keyspace := initPlotInfo(start, end, "Keyspace Popularity as a Function of Time", "Requests per second", path+"keyspace.png")
	//cacheReq := initPlotInfo(start, end, "Cache Requests as a Function of Time", "Requests per second", path+"cache_requests.png")
	//cacheSize := initPlotInfo(start, end, "Cache Sizes as a Function of Time", "Number of Items", path+"cacheSizes.png")
	//m.plotConfig(start, end, path+"config.png")
	//
	//tilePlots(path+"tiled.png", [][]string{
	//	{path + "config.png", allRequests.path},
	//	{keyspace.path, dbRequests.path},
	//	{latency.path, cacheHits.path},
	//	{cacheSize.path, cacheReq.path},
	//})
}

func (plt *plotInfo) makePlot() {
	duration := plt.end.Sub(plt.start)
	p := plot.New()
	p.Title.Text = plt.title
	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = plt.yAxis
	p.X.Min = 0.0
	p.X.Max = duration.Seconds()
	p.Y.Min = 0.0

	// Adjust legend position
	p.Legend.Top = true            // Position the legend at the top of the plot
	p.Legend.Left = true           // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
	p.Legend.YOffs = vg.Points(30) // Move the legend up

	// Define the resolution and calculate timeSlice
	resolution := float64(plt.numBuckets)
	timeSlice := time.Duration(float64(duration.Nanoseconds()) / resolution)

	for _, cat := range plt.categories {

		// Aggregate metrics into buckets based on the timeSlice
		countsPerSlice := make(map[int64]int)
		mtrcs := Filter(cat.filter)

		for _, m := range mtrcs {
			bucket := int64(math.Ceil(float64(m.timestamp.Sub(plt.start).Microseconds()) / float64(timeSlice.Microseconds())))
			countsPerSlice[bucket]++
		}

		// Create a plotter.XYs to hold the request counts
		pts := make(plotter.XYs, int(resolution))
		maxPerSecond := 0.0
		sum := 0.0
		count2 := 0.0

		// Fill the pts with the request counts
		for i := 0; i < int(resolution); i++ {
			if count, ok := countsPerSlice[int64(i)]; ok {
				countPerSecond := float64(count) / timeSlice.Seconds()
				maxPerSecond = math.Max(maxPerSecond, countPerSecond)
				pts[i].Y = countPerSecond
				sum += countPerSecond
				count2 += 1.0
			}
			pts[i].X = float64(i) * timeSlice.Seconds()
		}
		mean := sum / count2

		p.Y.Max = maxPerSecond * 1.2

		// Create a line chart
		line, err := plotter.NewLine(pts)
		if err != nil {
			log.Panic(err)
		}

		addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), cat.color)

		p.Add(line)

	}

	plt.plotNodeFailures(p)

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, plt.path); err != nil {
		log.Panic(err)
	}

}

func (plt *plotInfo) plotNodeFailures(p *plot.Plot) {

	duration := plt.end.Sub(plt.start)

	i := 0

	for _, node := range globalMetrics.config.Cache.Nodes {
		for _, interval := range node.FailureIntervals {
			iStart := time.Duration(interval.Start * float64(duration.Nanoseconds())).Seconds()
			iEnd := time.Duration(interval.End * float64(duration.Nanoseconds())).Seconds()
			if iStart < duration.Seconds() {
				addVerticalLine(p, iStart, fmt.Sprintf("node%d\nfailed", node.NodeId.Value), LIGHT_COLORS[i])
				if iEnd < duration.Seconds() {
					addVerticalLine(p, iEnd, fmt.Sprintf("node%d\nrecovered", node.NodeId.Value), LIGHT_COLORS[i])
				}
			}

		}
		i += 1
	}
}

func tilePlots(tiled string, fileNames [][]string) {
	// Define padding between images.
	padding := 20 // pixels

	// Load all the images and find out the max width and height.
	// Store nil for blanks to be filled in later.
	imagesArray := make([][]image.Image, len(fileNames))
	var maxWidth, maxHeight, maxRowLength int
	for i, row := range fileNames {
		imagesArray[i] = make([]image.Image, len(row))
		if len(row) > maxRowLength {
			maxRowLength = len(row) // Track the max row length to handle blanks.
		}
		for j, fileName := range row {
			if fileName == "" {
				imagesArray[i][j] = nil // If there's no filename, we'll insert a blank later.
				continue
			}
			img, err := openImage(fileName)
			if err != nil {
				log.Fatal(err)
			}
			imagesArray[i][j] = img
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
	for i, row := range imagesArray {
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
	defer func(outFile *os.File) {
		err := outFile.Close()
		if err != nil {
			panic(err)
		}
	}(outFile)
	err = png.Encode(outFile, tiledImg)
	if err != nil {
		panic(err)
	}

}

func plotConfig(start time.Time, end time.Time, filename string) {
	config := globalMetrics.config
	// Create a blank image with enough space
	img := image.NewRGBA(image.Rect(0, 0, 800, 400))

	// Fill the image with a background color (white)
	draw.Draw(img, img.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)

	leftIndent := 50

	failures := 0

	for _, node := range config.Cache.Nodes {
		failures += len(node.FailureIntervals)
	}

	// Draw the txt on the image
	addLabel(img, leftIndent+20, 40, "Configuration Summary:", "metrics/fonts/roboto/Roboto-Bold.ttf", 18.0)
	addLabel(img, leftIndent+40, 90, fmt.Sprintf("Duration: %d seconds", int(end.Sub(start).Seconds())), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 120, fmt.Sprintf("Target Requests per Second: %d", config.Performance.TargetOperationsPerSec.Value), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 150, fmt.Sprintf("Nodes: %d", len(config.Cache.Nodes)), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 180, fmt.Sprintf("Virtual Nodes: %d", config.Cache.VirtualNodes.Value), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 210, fmt.Sprintf("Read Percentage: %d%%", int(config.Workload.ReadProportion.Value*100.0)), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 240, fmt.Sprintf("Failures: %d", failures), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 270, fmt.Sprintf("Key Range: %d to %d", config.Workload.KeyRangeLowerBound.Value, config.Workload.KeyRangeLowerBound.Value+config.Performance.InsertCount.Value-1), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 300, fmt.Sprintf("Concurrency: %v", config.Performance.ThreadCount.Value), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 330, fmt.Sprintf("Warmup Time: %d", config.Measurements.WarmUpTime.Value), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)
	addLabel(img, leftIndent+40, 360, fmt.Sprintf("Request Distribution: %d", config.Workload.RequestDistribution.Value), "metrics/fonts/roboto/Roboto-Medium.ttf", 16.0)

	// Save the image to file
	saveImage(filename, img)

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

//
//
//func (plt *Plotter_) getCountsPerTimeSlice(metrics []Metric, filter func(Metric) bool) (plotter.XYs, float64, float64) {
//	// Define the resolution and calculate timeSlice
//	resolution := 30
//	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
//
//	countsPerSlice := make(map[int64]int)
//	for _, metric := range metrics {
//		if filter(metric) {
//			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(plt.m.start).Microseconds()) / float64(timeSlice.Microseconds())))
//			countsPerSlice[bucket]++
//		}
//	}
//	pts := make(plotter.XYs, resolution)
//	count_ := 0.0
//	sum := 0.0
//	maxCountPerSecond := 0.0
//
//	// Fill the pts with the request counts
//	for i := 0; i < resolution; i++ {
//		if count, ok := countsPerSlice[int64(i)]; ok {
//			countPerSecond := float64(count) / timeSlice.Seconds()
//			maxCountPerSecond = math.Max(maxCountPerSecond, countPerSecond)
//			pts[i].Y = countPerSecond
//			sum += countPerSecond
//			count_++
//		}
//		pts[i].X = float64(i) * timeSlice.Seconds()
//	}
//	if count_ > 0 {
//		return pts, maxCountPerSecond, sum / count_
//	}
//	return pts, maxCountPerSecond, 0
//}
//
//func (plt *Plotter_) getAveragePerTimeSlice(metrics []Metric, filter func(Metric) bool) (plotter.XYs, float64, float64) {
//	// Define the resolution and calculate timeSlice
//	resolution := 30
//	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
//	sumPerSlice := make(map[int64]int64)
//	countsPerSlice := make(map[int64]int)
//	averagePerSlice := make(map[int64]float64)
//
//	for _, metric := range metrics {
//		if filter(metric) {
//			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(plt.m.start).Microseconds()) / float64(timeSlice.Microseconds())))
//			countsPerSlice[bucket]++
//			sumPerSlice[bucket] += int64(metric.floatValues["size"])
//			averagePerSlice[bucket] = float64(sumPerSlice[bucket]) / float64(countsPerSlice[bucket])
//		}
//	}
//	pts := make(plotter.XYs, resolution)
//	count_ := 0.0
//	sum := 0.0
//	maxCountPerSecond := 0.0
//
//	// Fill the pts with the request counts
//	for i := 0; i < resolution; i++ {
//		if average, ok := averagePerSlice[int64(i)]; ok {
//			maxCountPerSecond = math.Max(maxCountPerSecond, average)
//			pts[i].Y = average
//			sum += average
//			count_++
//		}
//		pts[i].X = float64(i) * timeSlice.Seconds()
//	}
//	if count_ > 0 {
//		return pts, maxCountPerSecond, sum / count_
//	}
//	return pts, maxCountPerSecond, 0
//}
//
//func (plt *Plotter_) PlotDatabaseRequests(fileName string) {
//	start := plt.m.start
//	end := plt.m.end
//	p := plt.dbRequests
//	p.Title.Text = "Database Requests per Second as a Function of Time"
//	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
//	p.Title.TextStyle.Font.Size = 15
//	p.X.Label.Text = "Time (s)"
//	p.Y.Label.Text = "Requests per second"
//	p.X.Min = 0.0
//	p.X.Max = end.Sub(start).Seconds()
//	p.Y.Min = 0.0
//
//	// Adjust legend position
//	p.Legend.Top = true            // Position the legend at the top of the plot
//	p.Legend.Left = true           // Position the legend to the left side of the plot
//	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
//	p.Legend.YOffs = vg.Points(30) // Move the legend up
//
//	// Define the resolution and calculate timeSlice
//	resolution := 30
//	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
//
//	// Aggregate metrics into buckets based on the timeSlice
//	requestCountsPerSlice := make(map[int64]int)
//	metrics := plt.m.GetDatabaseRequests()
//	for _, metric := range metrics {
//		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
//		requestCountsPerSlice[bucket]++
//	}
//
//	// Create a plotter.XYs to hold the request counts
//	pts := make(plotter.XYs, resolution)
//	maxReqPerSecond := 0.0
//
//	// Fill the pts with the request counts
//	for i := 0; i < resolution; i++ {
//		if count, ok := requestCountsPerSlice[int64(i)]; ok {
//			reqPerSecond := float64(count) / timeSlice.Seconds()
//			maxReqPerSecond = math.Max(maxReqPerSecond, reqPerSecond)
//			pts[i].Y = reqPerSecond
//		}
//		pts[i].X = float64(i) * timeSlice.Seconds()
//	}
//	p.Y.Max = maxReqPerSecond * 1.2
//
//	// Create a line chart
//	line, err := plotter.NewLine(pts)
//	if err != nil {
//		log.Panic(err)
//	}
//
//	p.Add(line)
//
//	plt.plotNodeFailures(p)
//
//	// Save the plot to a PNG file
//	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
//		log.Panic(err)
//	}
//}
//
//func (plt *Plotter_) PlotKeyspacePopularities(fileName string) {
//	start := plt.m.start
//	end := plt.end
//	p := plt.keyspace
//	p.Title.Text = "Keyspace Popularity as a Function of Time"
//	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
//	p.Title.TextStyle.Font.Size = 15
//	p.X.Label.Text = "Time (s)"
//	p.Y.Label.Text = "Requests per second"
//	p.X.Min = 0.0
//	p.X.Max = end.Sub(start).Seconds()
//	p.Y.Min = 0.0
//
//	// Adjust legend position
//	p.Legend.Top = true            // Position the legend at the top of the plot
//	p.Legend.Left = true           // Position the legend to the left side of the plot
//	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
//	p.Legend.YOffs = vg.Points(30) // Move the legend up
//
//	metrics := plt.m.GetKeyspacePopularities()
//	for i := 0; i < len(plt.m.config.keyspacePop); i++ {
//		pts, maxCount, mean := plt.getCountsPerTimeSlice(metrics, func(metric Metric) bool {
//			return int(math.Round(metric.floatValues["keyspace"])) == i
//		})
//		p.Y.Max = math.Max(p.Y.Max, maxCount*1.2)
//		line, err := plotter.NewLine(pts)
//		if err != nil {
//			log.Panic(err)
//		}
//		line.Color = DARK_COLORS[i]
//		p.Add(line)
//		p.Legend.Add(fmt.Sprintf("keyspace %d", i), line)
//		addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), LIGHT_COLORS[i])
//	}
//
//	// Save the plot to a PNG file
//	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
//		log.Panic(err)
//	}
//}
//
//func (plt *Plotter_) PlotNodes(fileName string) {
//	start := plt.m.start
//	end := plt.end
//	p := plt.nodeRequests
//	p.Title.Text = "Cache Requests as a Function of Time"
//	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
//	p.Title.TextStyle.Font.Size = 15
//	p.X.Label.Text = "Time (s)"
//	p.Y.Label.Text = "Requests per second"
//	p.X.Min = 0.0
//	p.X.Max = end.Sub(start).Seconds()
//	p.Y.Min = 0.0
//
//	// Adjust legend position
//	p.Legend.Top = true            // Position the legend at the top of the plot
//	p.Legend.Left = true           // Position the legend to the left side of the plot
//	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
//	p.Legend.YOffs = vg.Points(30) // Move the legend up
//
//	metrics := plt.m.GetAllRequests()
//	for i := 0; i < len(plt.m.config.nodeConfigs); i++ {
//		pts, maxCount, mean := plt.getCountsPerTimeSlice(metrics, func(metric Metric) bool {
//			return int(math.Round(metric.floatValues["nodeIndex"])) == i
//		})
//		p.Y.Max = math.Max(p.Y.Max, maxCount*1.2)
//		line, err := plotter.NewLine(pts)
//		if err != nil {
//			log.Panic(err)
//		}
//		line.Color = DARK_COLORS[i]
//		p.Add(line)
//		p.Legend.Add(fmt.Sprintf("node%d", i+1), line)
//		addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), LIGHT_COLORS[i])
//	}
//
//	// Save the plot to a PNG file
//	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
//		log.Panic(err)
//	}
//}
//
//func (plt *Plotter_) PlotCacheSizes(fileName string) {
//	start := plt.m.start
//	end := plt.end
//	p := plt.cacheSizes
//	p.Title.Text = "Cache Sizes as a Function of Time"
//	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
//	p.Title.TextStyle.Font.Size = 15
//	p.X.Label.Text = "Time (s)"
//	p.Y.Label.Text = "Number of Items"
//	p.X.Min = 0.0
//	p.X.Max = end.Sub(start).Seconds()
//	p.Y.Min = 0.0
//
//	// Adjust legend position
//	p.Legend.Top = true            // Position the legend at the top of the plot
//	p.Legend.Left = true           // Position the legend to the left side of the plot
//	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
//	p.Legend.YOffs = vg.Points(30) // Move the legend up
//
//	metrics := plt.m.GetCacheSizes()
//	for i := 0; i < len(plt.m.config.nodeConfigs); i++ {
//		pts, maxCount, mean := plt.getAveragePerTimeSlice(metrics, func(metric Metric) bool {
//			return int(math.Round(metric.floatValues["nodeIndex"])) == i
//		})
//		p.Y.Max = math.Max(p.Y.Max, maxCount*1.2)
//		line, err := plotter.NewLine(pts)
//		if err != nil {
//			log.Panic(err)
//		}
//		line.Color = DARK_COLORS[i]
//		p.Add(line)
//		p.Legend.Add(fmt.Sprintf("node%d", i+1), line)
//		addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), LIGHT_COLORS[i])
//	}
//
//	// Save the plot to a PNG file
//	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
//		log.Panic(err)
//	}
//}
//
//func (plt *Plotter_) PlotAllRequests(fileName string) {
//	start := plt.m.start
//	end := plt.end
//	p := plt.allRequests
//	p.Title.Text = "Total Requests per Second As a Function of Time"
//	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
//	p.Title.TextStyle.Font.Size = 15
//	p.X.Label.Text = "Time (s)"
//	p.Y.Label.Text = "Requests per second"
//	p.X.Min = 0.0
//	p.X.Max = end.Sub(start).Seconds()
//	p.Y.Min = 0.0
//
//	// Adjust legend position
//	p.Legend.Top = true            // Position the legend at the top of the plot
//	p.Legend.Left = true           // Position the legend to the left side of the plot
//	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
//	p.Legend.YOffs = vg.Points(30) // Move the legend up
//
//	// Define the resolution and calculate timeSlice
//	resolution := 30
//	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
//
//	// Aggregate metrics into buckets based on the timeSlice
//	unsuccessfulRequestCountsPerSlice := make(map[int64]int)
//	successfulRequestCountsPerSlice := make(map[int64]int)
//	metrics := plt.m.GetAllRequests()
//	for _, metric := range metrics {
//		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
//		if metric.stringValues["successful"] == "true" {
//			successfulRequestCountsPerSlice[bucket]++
//		} else {
//			unsuccessfulRequestCountsPerSlice[bucket]++
//		}
//	}
//
//	// Create a plotter.XYs to hold the request counts
//	unsuccessfulPts := make(plotter.XYs, resolution)
//	successfulPts := make(plotter.XYs, resolution)
//	maxReqPerSecond := 0.0
//	sumReqPerSecond := 0.0
//	countSecs := 0
//
//	// Fill the unsuccessfulPts with the request counts
//	for i := 0; i < resolution; i++ {
//		if count, ok := unsuccessfulRequestCountsPerSlice[int64(i)]; ok {
//			reqPerSecond := float64(count) / timeSlice.Seconds()
//			maxReqPerSecond = math.Max(maxReqPerSecond, reqPerSecond)
//			sumReqPerSecond += reqPerSecond
//			countSecs++
//			unsuccessfulPts[i].Y = reqPerSecond
//		}
//		if count, ok := successfulRequestCountsPerSlice[int64(i)]; ok {
//			reqPerSecond := float64(count) / timeSlice.Seconds()
//			maxReqPerSecond = math.Max(maxReqPerSecond, reqPerSecond)
//			sumReqPerSecond += reqPerSecond
//			countSecs++
//			successfulPts[i].Y = reqPerSecond
//		}
//		unsuccessfulPts[i].X = float64(i) * timeSlice.Seconds()
//		successfulPts[i].X = float64(i) * timeSlice.Seconds()
//	}
//	p.Y.Max = maxReqPerSecond * 1.2
//
//	line2, err := plotter.NewLine(successfulPts)
//	if err != nil {
//		log.Panic(err)
//	}
//	line2.Color = DARK_GREEN
//	p.Legend.Add("successful", line2)
//	p.Add(line2)
//
//	line, err := plotter.NewLine(unsuccessfulPts)
//	if err != nil {
//		log.Panic(err)
//	}
//	line.Color = DARK_RED
//	p.Legend.Add("unsuccessful", line)
//	p.Add(line)
//
//	//mean := sumReqPerSecond / float64(countSecs)
//	//addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), GREY)
//
//	// Save the plot to a PNG file
//	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
//		log.Panic(err)
//	}
//}
//
//func (plt *Plotter_) PlotLatency(fileName string) {
//	start := plt.m.start
//	end := plt.end
//	p := plt.latency
//	p.Title.Text = "Request Latency As a Function of Time"
//	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
//	p.Title.TextStyle.Font.Size = 15
//	p.X.Label.Text = "Time (s)"
//	p.Y.Label.Text = "Average Latency (ms)"
//	p.X.Min = 0.0
//	p.X.Max = end.Sub(start).Seconds()
//	p.Y.Min = 0.0
//
//	// Adjust legend position
//	p.Legend.Top = true            // Position the legend at the top of the plot
//	p.Legend.Left = true           // Position the legend to the left side of the plot
//	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
//	p.Legend.YOffs = vg.Points(30) // Move the legend up
//
//	// Define the resolution and calculate timeSlice
//	resolution := 30
//	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
//
//	// Aggregate metrics into buckets based on the timeSlice
//	totalLatencyPerSlice := make(map[int64]float64)
//	countPerSlice := make(map[int64]int)
//	metrics := plt.m.GetLatency()
//	for _, metric := range metrics {
//		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Seconds() / float64(timeSlice.Seconds()))))
//		totalLatencyPerSlice[bucket] += metric.floatValues["latency"]
//		countPerSlice[bucket]++
//	}
//
//	averageLatencyPerSlice := make(map[int64]float64)
//
//	for i := 0; i < resolution; i++ {
//		if countPerSlice[int64(i)] > 0 {
//			averageLatencyPerSlice[int64(i)] = 1000 * totalLatencyPerSlice[int64(i)] / float64(countPerSlice[int64(i)])
//		} else {
//			averageLatencyPerSlice[int64(i)] = 0.0
//		}
//	}
//
//	// Create a plotter.XYs to hold the request counts
//	pts := make(plotter.XYs, resolution)
//	maxLatency := 0.0
//	sumLatency := 0.0
//	countLatency := 0
//
//	// Fill the pts with the request counts
//	for i := 0; i < resolution; i++ {
//		if latency, ok := averageLatencyPerSlice[int64(i)]; ok {
//			maxLatency = math.Max(maxLatency, latency)
//			sumLatency += latency
//			countLatency++
//			pts[i].Y = latency
//		} else {
//			pts[i].Y = 0.0
//		}
//		pts[i].X = float64(i) * timeSlice.Seconds()
//	}
//	p.Y.Max = maxLatency * 1.2
//
//	// Create a line chart
//	line, err := plotter.NewLine(pts)
//	if err != nil {
//		log.Panic(err)
//	}
//
//	p.Add(line)
//
//	mean := sumLatency / float64(countLatency)
//	addHorizontalLine(p, mean, fmt.Sprintf(" mean\n (%.2f ms)", mean), GREY)
//
//	plt.plotNodeFailures(p)
//
//	// Save the plot to a PNG file
//	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
//		log.Panic(err)
//	}
//}
//
//func (plt *Plotter_) PlotCacheHits(fileName string) {
//	start := plt.m.start
//	end := plt.end
//	p := plt.cacheHits
//	p.Title.Text = "Cache Hit Ratio as a Function of Time"
//	p.Title.Padding = vg.Points(30) // Increase the padding to create more space
//	p.Title.TextStyle.Font.Size = 15
//	// Adjust padding around the entire plot
//	p.X.Label.Padding = vg.Points(10)
//	p.Y.Label.Padding = vg.Points(10)
//
//	p.X.Label.Text = "Time (s)"
//	p.Y.Label.Text = "Cache Hit Ratio"
//	p.X.Min = 0.0
//	p.X.Max = end.Sub(start).Seconds()
//	p.Y.Min = 0.0
//	p.Y.Max = 1.0
//
//	// Adjust legend position
//	p.Legend.Top = true            // Position the legend at the top of the plot
//	p.Legend.Left = true           // Position the legend to the left side of the plot
//	p.Legend.XOffs = vg.Points(10) // Move the legend to the right
//	p.Legend.YOffs = vg.Points(30) // Move the legend up
//	p.Legend.TextStyle.Font.Size = 11
//
//	// Define the resolution and calculate timeSlice
//	resolution := 30
//	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
//
//	// Aggregate metrics into buckets based on the timeSlice
//	cacheHitsPerSlice := make(map[int64]int)
//	requestsPerSlice := make(map[int64]int)
//	metrics := plt.m.GetCacheHits()
//	requests := plt.m.GetAllRequests()
//	for _, metric := range metrics {
//		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
//		cacheHitsPerSlice[bucket]++
//	}
//	for _, metric := range requests {
//		bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
//		requestsPerSlice[bucket]++
//	}
//
//	// Create a plotter.XYs to hold the request counts
//	pts := make(plotter.XYs, resolution)
//
//	// Fill the pts with the request counts
//	for i := 0; i < resolution; i++ {
//		if hits, ok := cacheHitsPerSlice[int64(i)]; ok {
//			hitsPerSecond := float64(hits) / timeSlice.Seconds()
//			if req, ok2 := requestsPerSlice[int64(i)]; ok2 {
//				reqsPerSecond := float64(req) / timeSlice.Seconds()
//				hitRatio := hitsPerSecond / reqsPerSecond
//				pts[i].Y = hitRatio
//			}
//		}
//		pts[i].X = float64(i) * timeSlice.Seconds()
//	}
//
//	// Create a line chart
//	line, err := plotter.NewLine(pts)
//	if err != nil {
//		log.Panic(err)
//	}
//
//	p.Legend.Add("aggregate", line)
//	p.Add(line)
//
//	for i := 0; i < len(plt.m.config.nodeConfigs); i++ {
//		plt.PlotCacheHitsForNode(p, i)
//	}
//
//	plt.plotNodeFailures(p)
//
//	// Save the plot to a PNG file
//	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
//		log.Panic(err)
//	}
//}
//
//func (plt *Plotter_) plotNodeFailures(p *plot.Plot) {
//
//	nodeFailures := plt.m.GetFailureIntervals()
//
//	for i := 0; i < len(nodeFailures); i++ {
//		metric := nodeFailures[i]
//		iStart := metric.timestamp
//		duration := time.Duration(metric.floatValues["duration"] * float64(time.Second))
//		iEnd := iStart.Add(duration)
//		addVerticalLine(p, iStart.Sub(plt.m.start).Seconds(), fmt.Sprintf("node%d\nfailed", i+1), LIGHT_COLORS[i])
//		addVerticalLine(p, iEnd.Sub(plt.m.start).Seconds(), fmt.Sprintf("node%d\nrecovered", i+1), LIGHT_COLORS[i])
//		//fmt.Printf("node%d\nfailed from %d to %d\n", i+1, int(math.Round(iStart.Sub(plt.m.start).Seconds())), int(math.Round(iEnd.Sub(plt.m.start).Seconds())))
//	}
//}
//
//func (plt *Plotter_) PlotCacheHitsForNode(p *plot.Plot, nodeIndex int) {
//	start := plt.m.start
//
//	// Define the resolution and calculate timeSlice
//	resolution := 30
//	timeSlice := time.Duration(float64(plt.m.config.maxDuration.Nanoseconds()) / float64(resolution))
//
//	// Aggregate metrics into buckets based on the timeSlice
//	cacheHitsPerSlice := make(map[int64]int)
//	requestsPerSlice := make(map[int64]int)
//	metrics := plt.m.GetCacheHits()
//	requests := plt.m.GetAllRequests()
//	for _, metric := range metrics {
//		if int(metric.floatValues["nodeIndex"]) == nodeIndex {
//			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
//			cacheHitsPerSlice[bucket]++
//		}
//	}
//	for _, metric := range requests {
//		if int(metric.floatValues["nodeIndex"]) == nodeIndex {
//			bucket := int64(math.Ceil(float64(metric.timestamp.Sub(start).Microseconds()) / float64(timeSlice.Microseconds())))
//			requestsPerSlice[bucket]++
//		}
//	}
//
//	// Create a plotter.XYs to hold the request counts
//	pts := make(plotter.XYs, resolution)
//
//	// Fill the pts with the request counts
//	for i := 0; i < resolution; i++ {
//		if hits, ok := cacheHitsPerSlice[int64(i)]; ok {
//			hitsPerSecond := float64(hits) / timeSlice.Seconds()
//			if req, ok2 := requestsPerSlice[int64(i)]; ok2 {
//				reqsPerSecond := float64(req) / timeSlice.Seconds()
//				hitRatio := hitsPerSecond / reqsPerSecond
//				pts[i].Y = hitRatio
//			}
//		}
//		pts[i].X = float64(i) * timeSlice.Seconds()
//	}
//
//	// Create a line chart
//	line, err := plotter.NewLine(pts)
//	if err != nil {
//		log.Panic(err)
//	}
//
//	line.Color = DARK_COLORS[nodeIndex]
//	p.Legend.Add(fmt.Sprintf("node%d", nodeIndex+1), line)
//
//	p.Add(line)
//}
//
