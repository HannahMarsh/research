package metrics

import (
	"encoding/csv"
	"fmt"
	"github.com/benoitmasson/plotters/piechart"
	"github.com/dustin/go-humanize"
	"github.com/golang/freetype"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gonum.org/v1/gonum/interp"
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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	DARK_RED     = color.RGBA{R: 139, G: 0, B: 0, A: 255}     // Dark red
	DARK_ORANGE  = color.RGBA{R: 200, G: 100, B: 0, A: 255}   // Dark orange
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

	DARK_COLORS  = []color.RGBA{DARK_PINK, DARK_PURPLE, DARK_BLUE, DARK_GREEN, DARK_YELLOW, DARK_RED, DARK_ORANGE}
	LIGHT_COLORS = []color.RGBA{LIGHT_PINK, LIGHT_PURPLE, LIGHT_BLUE, LIGHT_GREEN, LIGHT_YELLOW, LIGHT_RED, DARK_ORANGE}
)

type plotInfo struct {
	categories       []category
	numBuckets       int
	title            string
	yAxis            string
	xAxis            string
	path             string
	csvPath          string
	start            time.Time
	end              time.Time
	showNodeFailures bool
	barchart         bool
}

type category struct {
	filters   []func(Metric) bool
	reduce    func([][]Metric, time.Duration) float64
	values    map[int64]int64
	plotLabel string
	color     color.RGBA
	showMean  bool
}

func has(m Metric, label string, b interface{}) bool {
	if a, exists := m.tags[label]; exists {
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
	}
	return false
}

func hasTag(m Metric, label string) bool {
	if _, exists := m.tags[label]; exists {
		return true
	}
	return false
}

func divideFirstBySecond(m [][]Metric, timeSlice time.Duration) float64 {
	first := len(m[0])
	second := len(m[1])
	if second == 0 {
		return 0
	}
	return float64(first) / float64(second)
}

func countPerSecond(m [][]Metric, timeSlice time.Duration) float64 {
	return float64(len(m[0])) / timeSlice.Seconds()
}

func totalCount(m [][]Metric, timeSlice time.Duration) float64 {
	return float64(len(m[0]))
}

func averageValue(value func(Metric) float64) func([][]Metric, time.Duration) float64 {
	return func(m [][]Metric, timeSlice time.Duration) float64 {
		sum_ := 0.0
		count := 0
		for _, mtrc := range m[0] {
			sum_ += value(mtrc)
			count++
		}
		if count == 0 {
			return 0.0
		}
		return sum_ / float64(count)
	}
}

func forEachNode(f func(int) category) []category {
	var nodeCategories []category
	for _, node := range globalConfig.Cache.Nodes {
		nodeIndex := node.NodeId.Value - 1
		nodeCategories = append(nodeCategories, f(nodeIndex))
	}
	return nodeCategories
}

func forEachNodeMulti(f func(int) []category) []category {
	var nodeCategories []category
	for _, node := range globalConfig.Cache.Nodes {
		nodeIndex := node.NodeId.Value - 1
		c := f(nodeIndex)
		for _, cc := range c {
			nodeCategories = append(nodeCategories, cc)
		}
	}
	return nodeCategories
}

func getPopularities(nodeIndex int, f func(map[int64]int64) category) category {
	pops := make(map[int64]int64)
	for _, pop := range KEYS[nodeIndex] {
		if _, ok := pops[pop]; !ok {
			pops[pop] = 0
		}
		pops[pop] += 1
	}
	return f(pops)
}

func PlotMetrics(s time.Time, e time.Time) {
	dataDir := globalConfig.Measurements.MetricsOutputDir.Value
	indPath := dataDir + globalConfig.Workload.WorkloadIdentifier.Value + "/"
	summaryPath := dataDir + globalConfig.Workload.WorkloadIdentifier.Value + "_summary.png"
	csvPath := indPath + "csv/"
	pngPath := indPath + "png/"
	if err := os.MkdirAll(pngPath, os.ModePerm); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(csvPath, os.ModePerm); err != nil {
		panic(err)
	}
	fmt.Printf("Plotting metrics...\n")
	numBuckets := 40
	start := s.Add(warmUptime)
	end := start.Add(time.Duration(globalConfig.Workload.TargetExecutionTime.Value+globalConfig.Measurements.WarmUpTime.Value) * time.Second)
	if e.Before(end) {
		end = e
	}
	end = end.Add(-1 * time.Second)

	var pi = []*plotInfo{
		{
			title: "Transaction Latency as a Function of Time",
			yAxis: "Latency (ms)",
			categories: []category{
				{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == WORKLOAD && hasTag(m, LATENCY)
						},
					},
					reduce:   averageValue(func(m Metric) float64 { return 1000 * m.tags[LATENCY].(float64) }),
					color:    DARK_BLUE,
					showMean: true,
				},
			},
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Workload as a Function of Time",
			yAxis: "Requests per second",
			categories: []category{
				{
					plotLabel: "Read Transactions",
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == WORKLOAD && has(m, OPERATION, READ)
						},
					},
					reduce:   countPerSecond,
					color:    DARK_PURPLE,
					showMean: false,
				},
				{
					plotLabel: "Insert Transactions",
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == WORKLOAD && has(m, OPERATION, INSERT)
						},
					},
					reduce:   countPerSecond,
					color:    DARK_ORANGE,
					showMean: false,
				},
			},
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Database Requests per Second as a Function of Time",
			yAxis: "Requests per second",
			categories: []category{
				{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == DATABASE_OPERATION && has(m, SUCCESSFUL, true)
						},
					},
					reduce:    countPerSecond,
					plotLabel: "Successful",
					color:     DARK_GREEN,
					showMean:  true,
				},
				{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == DATABASE_OPERATION && has(m, SUCCESSFUL, false)
						},
					},
					reduce:    countPerSecond,
					plotLabel: "Unsuccessful",
					color:     DARK_RED,
					showMean:  true,
				},
			},
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Database Latency as a Function of Time",
			yAxis: "Latency (ms)",
			categories: []category{
				{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == DATABASE_OPERATION && hasTag(m, LATENCY)
						},
					},
					reduce:   averageValue(func(m Metric) float64 { return 1000 * m.tags[LATENCY].(float64) }),
					color:    DARK_BLUE,
					showMean: true,
				},
			},
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Cache Requests Per Node as a Function of Time",
			yAxis: "Requests per second",
			categories: forEachNode(func(nodeIndex int) category {
				return category{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, NODE_INDEX, nodeIndex) && has(m, OPERATION, READ)
						},
					},
					reduce:    countPerSecond,
					plotLabel: fmt.Sprintf("Node%d", nodeIndex+1),
					color:     DARK_COLORS[nodeIndex],
					showMean:  false,
				}
			}),
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "All Cache Requests as a Function of Time",
			yAxis: "Requests per second",
			categories: []category{
				{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, SUCCESSFUL, true)
						},
					},
					reduce:    countPerSecond,
					plotLabel: "Total Hits Per Second",
					color:     DARK_GREEN,
					showMean:  false,
				},
				{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, SUCCESSFUL, false)
						}},
					reduce:    countPerSecond,
					plotLabel: "Total Misses Per Second",
					color:     DARK_RED,
					showMean:  false,
				},
			},
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},

		{
			title: "Cache Node Latency as a Function of Time",
			yAxis: "Latency (ms)",
			categories: forEachNode(func(nodeIndex int) category {
				return category{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, NODE_INDEX, nodeIndex) && hasTag(m, LATENCY)
						},
					},
					reduce:    averageValue(func(m Metric) float64 { return 1000 * m.tags[LATENCY].(float64) }),
					plotLabel: fmt.Sprintf("Node%d", nodeIndex+1),
					color:     DARK_COLORS[nodeIndex],
					showMean:  false,
				}
			}),
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Reverse CDF of Key Popularity Distribution Per Node",
			yAxis: "Reverse Cumulative Frequency",
			xAxis: "Key Popularity",
			categories: forEachNode(func(nodeIndex int) category {
				return getPopularities(nodeIndex, func(pops map[int64]int64) category {
					return category{
						values:    pops,
						plotLabel: fmt.Sprintf("Node%d", nodeIndex+1),
						color:     DARK_COLORS[nodeIndex],
						showMean:  false,
					}
				})
			}),
			start:      start,
			end:        end,
			numBuckets: numBuckets,
			barchart:   true,
		},
		{
			title: "Cache Hits Ratio as a Function of Time",
			yAxis: "Average Hit Ratio per Second",
			categories: forEachNode(func(nodeIndex int) category {
				return category{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, SUCCESSFUL, true) && has(m, NODE_INDEX, nodeIndex)
						},
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, NODE_INDEX, nodeIndex)
						},
					},
					reduce:    divideFirstBySecond,
					plotLabel: fmt.Sprintf("Node%d", nodeIndex+1),
					color:     DARK_COLORS[nodeIndex],
					showMean:  false,
				}
			}),
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Cache Size as a Function of Time",
			yAxis: "Number of Items in Cache",
			categories: forEachNode(func(nodeIndex int) category {
				return category{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, NODE_INDEX, nodeIndex)
						},
					},
					reduce:    averageValue(func(m Metric) float64 { return float64(m.tags[SIZE].(int64)) }),
					plotLabel: fmt.Sprintf("Node%d", nodeIndex+1),
					color:     DARK_COLORS[nodeIndex],
					showMean:  false,
				}
			}),
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Number of \"Hot\" (Top 1000 Most Popular) Key Requests Per Second as a Function of Time",
			yAxis: "Num Requests",
			categories: forEachNode(func(nodeIndex int) category {
				return category{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == CACHE_OPERATION && has(m, NODE_INDEX, nodeIndex) && has(m, HOTTEST, true)
						},
					},
					reduce:    countPerSecond,
					plotLabel: fmt.Sprintf("Node%d", nodeIndex+1),
					color:     DARK_COLORS[nodeIndex],
					showMean:  false,
				}
			}),
			start:            start,
			end:              end,
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
		{
			title: "Proportion of Read Transactions that go to the Database as a Function of Time",
			yAxis: "Fraction of Read Requests",
			categories: []category{
				{
					filters: []func(m Metric) bool{
						func(m Metric) bool {
							return m.metricType == TRANSACTION && (has(m, OPERATION, READ) || has(m, OPERATION, BATCH_READ) || has(m, OPERATION, SCAN)) && has(m, DATABASE, true)
						},
						func(m Metric) bool {
							return m.metricType == TRANSACTION && (has(m, OPERATION, READ) || has(m, OPERATION, BATCH_READ) || has(m, OPERATION, SCAN))
						},
					},
					reduce:   divideFirstBySecond,
					color:    DARK_BLUE,
					showMean: true,
				},
			},
			start:            start,
			end:              end,
			path:             indPath + "individual/db_request_proportion.png",
			numBuckets:       numBuckets,
			showNodeFailures: true,
		},
	}
	cols := 3
	rows := int(math.Ceil(float64(len(pi)+1) / float64(cols)))
	var piPath [][]string
	curIndex := 0

	for r := 0; r < rows; r++ {
		var row []string
		for c := 0; c < cols; c++ {
			if r == 0 && c == 0 {
				configPath := pngPath + "00-Config_Summary.png"
				plotConfig(start, end, configPath)
				row = append(row, configPath)
			} else {
				if curIndex < len(pi) {
					fmt.Printf("\t(%d/%d): %s\n", curIndex+1, len(pi), pi[curIndex].title)
					// pi[curIndex].indPath = indPath + "individual/" + strings.Replace(toTitleCase(strings.TrimSuffix(strings.ToLower(pi[curIndex].title), " as a function of time")), " ", "_", -1) + ".png"

					var xAxis = "distribution per node"
					if !pi[curIndex].barchart {
						xAxis = "as a function of time"
					}
					pi[curIndex].path = fmt.Sprintf("%s%02d-", pngPath, curIndex+1) + replace(toTitleCase(replace(strings.ToLower(pi[curIndex].title), "[\\)\\s]+"+xAxis, "")), "[\\s\\(\\)]+", "_") + ".png"
					pi[curIndex].csvPath = fmt.Sprintf("%s%02d-", csvPath, curIndex+1) + replace(toTitleCase(replace(strings.ToLower(pi[curIndex].title), "[\\)\\s]+"+xAxis, "")), "[\\s\\(\\)]+", "_") + ".csv"

					row = append(row, pi[curIndex].path)
					if pi[curIndex].barchart {
						pi[curIndex].makeCDFPlot()
					} else {
						pi[curIndex].makePlot()
					}
					curIndex += 1
				}
			}

		}
		piPath = append(piPath, row)
	}

	tilePlots(summaryPath, piPath)

	fmt.Printf("Summary plot saved to %s\n", summaryPath)
	getDBErrors()
	getCacheErrors()

}

func replace(originalString string, pattern string, replacement string) string {
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(originalString, replacement)
}

func toTitleCase(str string) string {
	caser := cases.Title(language.English, cases.NoLower)
	return caser.String(str)
}

func getDBErrors() {
	fmt.Printf("Database Errors:\n")
	errs := make(map[string]int64)
	if mtrcs := Filter(func(m Metric) bool {
		return m.metricType == DATABASE_OPERATION && hasTag(m, ERROR) && !has(m, ERROR, nil) && !has(m, ERROR, "")
	}); mtrcs != nil {
		for _, m := range mtrcs {
			if err, ok := m.tags[ERROR].(string); ok {
				if _, exists := errs[err]; !exists {
					errs[err] = 0
				}
				errs[err] += 1
			}
		}
	}
	for err, count := range errs {
		fmt.Printf("%s: %d\n", err, count)
	}

	var values []float64
	var labels []string
	for err, count := range errs {
		fmt.Printf("%s: %d\n", err, count)
		values = append(values, float64(count))
		labels = append(labels, fmt.Sprintf("\"%s\" (%s)", err, humanize.Comma(count)))
	}

	makePieChart("data/db_errors.png", "Database Errors", values, labels)

}

func getCacheErrors() {
	fmt.Printf("\nCache Errors:\n")
	errs := make(map[string]int64)
	if mtrcs := Filter(func(m Metric) bool {
		return m.metricType == CACHE_OPERATION && hasTag(m, ERROR) && !has(m, ERROR, nil) && !has(m, ERROR, "")
	}); mtrcs != nil {
		for _, m := range mtrcs {
			if err, ok := m.tags[ERROR].(string); ok {
				if _, exists := errs[err]; !exists {
					errs[err] = 0
				}
				errs[err] += 1
			}
		}
	}

	var values []float64
	var labels []string
	for err, count := range errs {
		fmt.Printf("%s: %d\n", err, count)
		values = append(values, float64(count))
		labels = append(labels, fmt.Sprintf("\"%s\" (%s)", err, humanize.Comma(count)))
	}

	makePieChart("data/cache_errors.png", "Cache Errors", values, labels)

}

func makePieChart(fileName string, title string, values plotter.Values, labels []string) {
	if len(values) == 0 {
		return
	}
	colors := []color.Color{
		color.RGBA{R: 245, G: 131, B: 199, A: 255}, // #f583c7
		color.RGBA{R: 218, G: 159, B: 214, A: 255}, // #da9fd6
		color.RGBA{R: 186, G: 186, B: 221, A: 255}, // #babadd
		color.RGBA{R: 155, G: 210, B: 232, A: 255}, // #9bd2e8
		color.RGBA{R: 126, G: 238, B: 245, A: 255}, // #7eeef5
	}

	extraPadding := 10.0 + float64(math.Min(float64(len(colors)), float64(len(values))))*10.0
	p := plot.New()
	p.Legend.Top = true                           // Position the legend at the top of the plot
	p.Legend.Left = true                          // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(5)                 // Move the legend to the right
	p.Legend.YOffs = vg.Points(extraPadding - 10) // Move the legend up
	p.HideAxes()

	p.Title.Text = title
	p.Title.TextStyle.Font.Size = 15
	p.Title.Padding = vg.Points(extraPadding) // Increase the padding to create more space

	total := int64(0)
	for _, v := range values {
		total += int64(v)
	}

	sofar := 0

	// Create a struct to hold pairs of values and labels
	type valueLabelPair struct {
		Value float64
		Label string
	}

	// Create a slice of pairs
	pairs := make([]valueLabelPair, len(values))
	for i, value := range values {
		pairs[i] = valueLabelPair{Value: value, Label: labels[i]}
	}

	// Sort the pairs slice in descending order of Value
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Value > pairs[j].Value
	})

	// Extract the sorted values and labels
	for i, pair := range pairs {
		values[i] = pair.Value
		labels[i] = pair.Label
	}

	for i := 0; i < len(colors)-1; i++ {
		if i < len(values) {
			// Create a pie chart
			pie, err := piechart.NewPieChart(plotter.Values{values[i]})
			if err != nil {
				log.Panic(err)
			}
			pie.Total = float64(total)
			pie.Offset.Value = float64(sofar)
			sofar += int(values[i])

			// Only add label if the slice is larger than our threshold
			percentage := values[i] / float64(total)
			if percentage >= 0.03 {
				pie.Labels.Nominal = []string{fmt.Sprintf("%d%%: %s", int(percentage*100), humanize.Comma(int64(int(values[i]))))}
				pie.Labels.Values.Show = false
				pie.Labels.Values.Percentage = false
				pie.Labels.Position = 0.5
			} else {
				pie.Labels.Nominal = []string{""}
				pie.Labels.Values.Show = false
				pie.Labels.Values.Percentage = false
			}

			pie.Color = colors[i]
			p.Add(pie)
			p.Legend.Add(labels[i], pie)
		} else {
			break
		}
	}
	if (len(colors) - 1) < len(values) {
		// Create a pie chart
		pie, err := piechart.NewPieChart(plotter.Values{float64(total) - float64(sofar)})
		if err != nil {
			log.Panic(err)
		}
		pie.Total = float64(total)
		pie.Offset.Value = float64(sofar)

		// Only add label if the slice is larger than our threshold
		percentage := float64(total-int64(sofar)) / float64(total)
		if percentage >= 0.03 {
			pie.Labels.Nominal = []string{fmt.Sprintf("%s", humanize.Comma(total-int64(sofar)))}
			pie.Labels.Values.Show = true
			pie.Labels.Values.Percentage = true
			pie.Labels.Position = 0.5
		} else {
			pie.Labels.Values.Show = false
		}

		pie.Color = colors[len(colors)-1]
		p.Add(pie)
		p.Legend.Add("Other", pie)
	}

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, fileName); err != nil {
		log.Panic(err)
	}
}

func (plt *plotInfo) makePlot() {

	extraPadding := 10.0 + float64(len(plt.categories))*10.0

	duration := plt.end.Sub(plt.start)
	p := plot.New()
	p.Title.Text = plt.title
	p.Title.Padding = vg.Points(extraPadding) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = plt.yAxis
	p.X.Min = 0.0
	p.X.Max = duration.Seconds()
	p.Y.Min = 0.0

	// Adjust legend position
	p.Legend.Top = true                           // Position the legend at the top of the plot
	p.Legend.Left = true                          // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(5)                 // Move the legend to the right
	p.Legend.YOffs = vg.Points(extraPadding - 10) // Move the legend up

	// Define the resolution and calculate timeSlice
	resolution := float64(plt.numBuckets)
	timeSlice := time.Duration(float64(duration.Nanoseconds()) / resolution)

	data := make(map[string]plotter.XYs)

	for _, cat := range plt.categories {

		mtrcsMultiple := make(map[int64][][]Metric)
		filterIndex := 0
		for _, filter := range cat.filters {
			if mtrcs := Filter(filter); mtrcs != nil {
				for _, m := range mtrcs {
					if m.timestamp.Before(plt.start) {
						continue
					}
					if m.timestamp.After(plt.end) {
						continue
					}
					bucket := int64(math.Ceil(float64(m.timestamp.Sub(plt.start).Nanoseconds()) / float64(timeSlice.Nanoseconds())))
					// Initialize the inner slice if it hasn't been already
					if _, ok := mtrcsMultiple[bucket]; !ok {
						mtrcsMultiple[bucket] = make([][]Metric, len(cat.filters))
					}
					mtrcsMultiple[bucket][filterIndex] = append(mtrcsMultiple[bucket][filterIndex], m)
				}
			}
			filterIndex++
		}
		pts := make(plotter.XYs, int(resolution))
		maxY := 0.0
		count := 0
		sum_ := 0.0
		for i := 0; i < int(resolution); i++ {
			if mtrcs, ok := mtrcsMultiple[int64(i)]; ok {
				pts[i].Y = cat.reduce(mtrcs, timeSlice)
				if math.IsNaN(pts[i].Y) {
					pts[i].Y = 0
				}
				maxY = math.Max(maxY, pts[i].Y)
				sum_ += pts[i].Y
				count++
			} else {
				pts[i].Y = 0
			}
			pts[i].X = float64(i) * timeSlice.Seconds()
		}
		mean := sum_ / float64(count)
		p.Y.Max = math.Max(p.Y.Max, maxY*1.2)

		//filename := plt.csvPath + "-" + replace(cat.plotLabel, "[,\\\\\\/\\s\\(\\)]+", "_") + ".csv"
		if cat.plotLabel == "" {
			data[p.Y.Label.Text] = pts
		} else {
			data[cat.plotLabel] = pts
		}
		pts = getSmooth(pts)

		//exportCategoryDataToCSV(cat, pts, filename)
		if line, err := plotter.NewLine(pts); err == nil {
			line.Color = cat.color
			p.Add(line)

			if cat.showMean && !math.IsNaN(mean) {
				if mean > 0 && mean < 1.0 {
					addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.2f)", mean), cat.color)
				} else {
					addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), cat.color)
				}

			} else if !cat.showMean {
				if !math.IsNaN(mean) {
					if mean > 0 && mean < 1.0 {
						cat.plotLabel += fmt.Sprintf(", (mean = %.2f)", mean)
					} else {
						cat.plotLabel += fmt.Sprintf(", (mean = %.0f)", mean)
					}
				} else {
					cat.plotLabel += ", (mean = 0)"
				}
			}

			if cat.plotLabel != "" {
				p.Legend.Add(cat.plotLabel, line)
			}

		} else {
			log.Panic(err)
		}
	}

	if plt.showNodeFailures {
		plt.plotNodeFailures(p)
	}

	height := vg.Length(4.0 * (1 + (0.03 * float64(len(plt.categories)))))

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, height*vg.Inch, plt.path); err != nil {
		log.Panic(err)
	}

	exportCategoryDataToCSV(data, plt.csvPath)

}

func (plt *plotInfo) makeCDFPlot() {

	extraPadding := float64(len(plt.categories)) * 7.0

	p := plot.New()
	p.Title.Text = plt.title

	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = plot.LogTicks{} // This will format the ticks appropriately for log scale
	p.X.Scale = plot.LogScale{}
	p.X.Tick.Marker = plot.LogTicks{} // This will format the ticks appropriately for log scale

	p.Title.Padding = vg.Points(extraPadding) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = plt.xAxis
	p.Y.Label.Text = plt.yAxis
	p.X.Min = 1.0
	p.Y.Min = 1.0

	yMin := 1.0

	// Adjust legend position
	p.Legend.Top = true                           // Position the legend at the top of the plot
	p.Legend.Left = true                          // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(5)                 // Move the legend to the right
	p.Legend.YOffs = vg.Points(extraPadding - 15) // Move the legend up

	hasPlotters := false

	data := make(map[string]plotter.XYs)

	max_ := float64(0)

	for _, cat := range plt.categories {
		for k, _ := range cat.values {
			max_ = math.Max(float64(k), max_)
		}
	}

	for _, cat := range plt.categories {

		totalKeys := 0.0
		numKeys := 0.0

		for _, v := range cat.values {
			totalKeys += float64(v)
			numKeys++
		}

		var pts plotter.XYs
		cumulativeCount := 0.0

		for i := int64(max_); i > int64(0); i-- {
			v, exists := cat.values[i]
			if !exists || v < 0 {
				v = 0
			}
			cumulativeCount += float64(v)

			if cumulativeCount <= 0 {
				cumulativeCount = 1.0
			}

			yMin = math.Min(yMin, cumulativeCount/totalKeys)
			X := float64(i)
			Y := float64(cumulativeCount / totalKeys)
			if !math.IsNaN(X) && !math.IsNaN(Y) && !math.IsInf(X, 1) && !math.IsInf(Y, 1) {
				pts = append(pts, plotter.XY{X: X, Y: Y})
			}
		}

		if cat.plotLabel == "" {
			data[p.Y.Label.Text] = pts
		} else {
			data[cat.plotLabel] = pts
		}

		//pts = getSmoothn(pts, 10000)

		mean := float64(totalKeys) / float64(numKeys)

		//exportCategoryDataToCSV(cat, pts, filename)
		if pts != nil {
			if line, err := plotter.NewLine(pts); err == nil {
				hasPlotters = true
				line.Color = cat.color
				p.Add(line)

				if cat.showMean && !math.IsNaN(mean) {
					if mean > 0 && mean < 1.0 {
						addHorizontalLine(p, mean, fmt.Sprintf("mean popularity per key\n(%.2f)", mean), cat.color)
					} else {
						addHorizontalLine(p, mean, fmt.Sprintf("mean popularity per key\n(%.0f)", mean), cat.color)
					}

				} else if !cat.showMean {
					if !math.IsNaN(mean) {
						if mean > 0 && mean < 1.0 {
							cat.plotLabel += fmt.Sprintf(", (mean popularity per key = %.2f)", mean)
						} else {
							cat.plotLabel += fmt.Sprintf(", (mean popularity per key = %.0f)", mean)
						}
					} else {
						cat.plotLabel += ", (mean = 0)"
					}
				}

				if cat.plotLabel != "" {
					p.Legend.Add(cat.plotLabel, line)
				}

			} else {
				log.Panic(err)
			}
		}
	}

	height := vg.Length(4.0 * (1 + (0.03 * float64(len(plt.categories)))))

	p.X.Min = 1.0
	// Save the plot to a PNG file
	if hasPlotters {
		if err := p.Save(8*vg.Inch, height*vg.Inch, plt.path); err != nil {
			log.Panic(err)
		}

		exportCategoryDataToCSV(data, plt.csvPath)
	}
}

func (plt *plotInfo) makeBarChart() {

	extraPadding := 10.0 + float64(len(plt.categories))*10.0

	p := plot.New()
	p.Title.Text = plt.title

	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = plot.LogTicks{} // This will format the ticks appropriately for log scale
	p.X.Scale = plot.LogScale{}
	p.X.Tick.Marker = plot.LogTicks{} // This will format the ticks appropriately for log scale

	p.Title.Padding = vg.Points(extraPadding) // Increase the padding to create more space
	p.Title.TextStyle.Font.Size = 15
	p.X.Label.Text = plt.xAxis
	p.Y.Label.Text = plt.yAxis
	p.X.Min = 1.0
	p.Y.Min = 1.0

	// Adjust legend position
	p.Legend.Top = true                           // Position the legend at the top of the plot
	p.Legend.Left = true                          // Position the legend to the left side of the plot
	p.Legend.XOffs = vg.Points(5)                 // Move the legend to the right
	p.Legend.YOffs = vg.Points(extraPadding - 10) // Move the legend up

	data := make(map[string]plotter.XYs)

	for _, cat := range plt.categories {

		count := 0
		sum_ := int64(0)
		index := 0
		pts := make(plotter.XYs, len(cat.values))
		// Extract keys to a slice
		keys := make([]int64, 0, len(cat.values))
		for k := range cat.values {
			keys = append(keys, k)
		}

		// Sort the keys slice
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		for _, key := range keys {
			v := cat.values[key]
			count++
			sum_ += v
			pts[index].X = float64(key)
			pts[index].Y = float64(v)
			index++
		}
		if cat.plotLabel == "" {
			data[p.Y.Label.Text] = pts
		} else {
			data[cat.plotLabel] = pts
		}

		pts = getSmooth(pts)

		mean := float64(sum_) / float64(count)

		//exportCategoryDataToCSV(cat, pts, filename)
		if line, err := plotter.NewLine(pts); err == nil {
			line.Color = cat.color
			p.Add(line)

			if cat.showMean && !math.IsNaN(mean) {
				if mean > 0 && mean < 1.0 {
					addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.2f)", mean), cat.color)
				} else {
					addHorizontalLine(p, mean, fmt.Sprintf("mean\n(%.0f)", mean), cat.color)
				}

			} else if !cat.showMean {
				if !math.IsNaN(mean) {
					if mean > 0 && mean < 1.0 {
						cat.plotLabel += fmt.Sprintf(", (mean = %.2f)", mean)
					} else {
						cat.plotLabel += fmt.Sprintf(", (mean = %.0f)", mean)
					}
				} else {
					cat.plotLabel += ", (mean = 0)"
				}
			}

			if cat.plotLabel != "" {
				p.Legend.Add(cat.plotLabel, line)
			}

		} else {
			log.Panic(err)
		}
	}

	height := vg.Length(4.0 * (1 + (0.03 * float64(len(plt.categories)))))

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, height*vg.Inch, plt.path); err != nil {
		log.Panic(err)
	}

	exportCategoryDataToCSV(data, plt.csvPath)

}

func (plt *plotInfo) plotNodeFailures(p *plot.Plot) {

	duration := plt.end.Sub(plt.start)

	m := globalAllMetrics.Filter(func(m Metric) bool {
		return m.metricType == NODE_FAILURE
	})

	for _, node := range globalConfig.Cache.Nodes {
		if node.FailureIntervals != nil && len(node.FailureIntervals) > 0 {
			for _, mm := range m.Filter(func(m Metric) bool { return has(m, NODE_INDEX, node.NodeId.Value-1) && has(m, INTERVAL, START) }) {
				if iStart := time.Duration(mm.timestamp.Sub(plt.start).Nanoseconds()).Seconds(); iStart < duration.Seconds() {
					addVerticalLine(p, iStart, fmt.Sprintf("node%d\nfailed\n(t = %.2f)", node.NodeId.Value, iStart), LIGHT_COLORS[node.NodeId.Value-1])
				}
			}
			for _, mm := range m.Filter(func(m Metric) bool { return has(m, NODE_INDEX, node.NodeId.Value-1) && has(m, INTERVAL, END) }) {
				if iEnd := time.Duration(mm.timestamp.Sub(plt.start).Nanoseconds()).Seconds(); iEnd < duration.Seconds() {
					addVerticalLine(p, iEnd, fmt.Sprintf("node%d\nrecovered\n(t = %.2f)", node.NodeId.Value, iEnd), LIGHT_COLORS[node.NodeId.Value-1])
				}
			}
		}
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
				continue
				//log.Fatal(err)
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
	config := globalConfig
	// Create a blank image with enough space
	img := image.NewRGBA(image.Rect(0, 0, 800, 400))

	// Fill the image with a background color (white)
	draw.Draw(img, img.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)

	leftIndent := 50

	failures := 0

	for _, node := range config.Cache.Nodes {
		failures += len(node.FailureIntervals)
	}

	cur := 0

	var add = func(left int, top int, bold bool, fontSize float64, str string) {
		font := "metrics/fonts/roboto/Roboto-Medium.ttf"
		if bold {
			font = "metrics/fonts/roboto/Roboto-Bold.ttf"
		}
		cur += top
		addLabel(img, leftIndent+left, cur, str, font, fontSize)
	}
	titleLeftSpacing := 15
	titleTopSpacing := 40
	leftSpacing := 30
	topSpacing := 30
	add(titleLeftSpacing, titleTopSpacing, true, 18.0, "Configuration Summary:")
	add(leftSpacing, int(float64(topSpacing)*1.5), false, 16.0, fmt.Sprintf("Workload ID: %s", toTitleCase(globalConfig.Workload.WorkloadIdentifier.Value)))
	add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Duration: %d seconds", int(end.Sub(start).Seconds())))
	add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Warmup Time: %d seconds", config.Measurements.WarmUpTime.Value))
	add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Target Requests per Second: %d", config.Workload.TargetOperationsPerSec.Value))
	add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Approx. Total Requests: %d", int(float64(config.Workload.TargetOperationsPerSec.Value)*end.Sub(start).Seconds())))
	add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Read Percentage: %d%%", int((1.0-config.Workload.InsertProportion.Value)*100.0)))
	add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Key Range: [0 to %d]", config.Workload.NumUniqueKeys.Value-1))
	add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Request Distribution: %s", toTitleCase(config.Workload.RequestDistribution.Value)))
	if config.Workload.RequestDistribution.Value == "zipfian" {
		add(leftSpacing, topSpacing, false, 16.0, fmt.Sprintf("Zipfian Constant: %.2f", config.Workload.ZipfianConstant.Value))
	}

	cur = 0
	secondIndent := 350
	add(titleLeftSpacing+secondIndent, titleTopSpacing, true, 18.0, "Cache Nodes:")
	add(leftSpacing+secondIndent, int(float64(topSpacing)*1.5), false, 16.0, "Type: Redis")
	add(leftSpacing+secondIndent, topSpacing, false, 16.0, fmt.Sprintf("Number of Nodes: %d", len(config.Cache.Nodes)))
	add(leftSpacing+secondIndent, topSpacing, false, 16.0, fmt.Sprintf("Number of Virtual Nodes: %d", config.Cache.VirtualNodes.Value))
	add(leftSpacing+secondIndent, topSpacing, false, 16.0, fmt.Sprintf("Number of Failures: %d", failures))
	add(leftSpacing+secondIndent, topSpacing, false, 16.0, fmt.Sprintf("Max memory per node (mbs): %d", config.Cache.Nodes[0].MaxMemoryMbs.Value))
	add(leftSpacing+secondIndent, topSpacing, false, 16.0, fmt.Sprintf("Max memory eviction policy: %s", config.Cache.Nodes[0].MaxMemoryPolicy.Value))

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
	labels.Offset = vg.Point{X: 5, Y: 5} // Adjust the X offset to move label closer to the line

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

func exportCategoryDataToCSV(data map[string]plotter.XYs, filename string) {
	// Create or truncate the file
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			panic(err)
		}
	}(file)

	// Create a new CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"Time (seconds)"}

	d := make(map[float64][]string)

	for cat, pts := range data {
		header = append(header, replace(cat, ", \\(mean.*", ""))

		l := 0
		// Write the data points
		for _, pt := range pts { // pts should be the data points for this category
			if _, ok := d[pt.X]; !ok {
				d[pt.X] = []string{strconv.FormatFloat(pt.X, 'f', -1, 64)}
			}
			d[pt.X] = append(d[pt.X], strconv.FormatFloat(pt.Y, 'f', -1, 64))
			l = max(l, len(d[pt.X]))
		}
		for k, v := range d {
			if len(v) < l {
				d[k] = append(v, strconv.FormatFloat(0.0, 'f', -1, 64))
			}
		}
	}

	// Write the headers
	if err = writer.Write(header); err != nil {
		log.Fatalf("Failed to write header to CSV: %v", err)
	}

	var keys []float64
	for k := range d {
		keys = append(keys, k)
	}

	// Sort the keys
	sort.Float64s(keys)

	// Iterate over the sorted keys and access the values
	for _, k := range keys {
		record := d[k]
		if err = writer.Write(record); err != nil {
			log.Fatalf("Failed to write records to CSV: %v", err)
		}
	}
}

func getSmooth(pts plotter.XYs) plotter.XYs {
	// Sort the points by X values.
	sort.Slice(pts, func(i, j int) bool {
		return pts[i].X < pts[j].X
	})

	// Extract X and Y values from the points.
	xs := make([]float64, len(pts))
	ys := make([]float64, len(pts))
	for i, pt := range pts {
		xs[i] = pt.X
		ys[i] = pt.Y
	}

	// Create AkimaSpline manually.
	var interpolator interp.AkimaSpline

	if err := interpolator.Fit(xs, ys); err != nil {
		panic(err)
	}

	// Number of points for the smooth curve.
	numPoints := 1000

	// Calculate the range and step for new X values.
	xMin := xs[0]
	xMax := xs[len(xs)-1]
	step := (xMax - xMin) / float64(numPoints-1)

	// Generate new points.
	newPts := make(plotter.XYs, numPoints)
	for i := 0; i < numPoints; i++ {
		newX := xMin + float64(i)*step
		newPts[i].X = newX
		newPts[i].Y = interpolator.Predict(newX)
	}

	return newPts
}

func getSmoothn(pts plotter.XYs, n int) plotter.XYs {
	// Sort the points by X values.
	sort.Slice(pts, func(i, j int) bool {
		return pts[i].X < pts[j].X
	})

	// Extract X and Y values from the points.
	xs := make([]float64, len(pts))
	ys := make([]float64, len(pts))
	for i, pt := range pts {
		xs[i] = pt.X
		ys[i] = pt.Y
	}

	// Create AkimaSpline manually.
	var interpolator interp.AkimaSpline

	if err := interpolator.Fit(xs, ys); err != nil {
		panic(err)
	}

	// Number of points for the smooth curve.
	numPoints := 1000

	// Calculate the range and step for new X values.
	xMin := xs[0]
	xMax := xs[len(xs)-1]
	step := math.Max(0.0000001, (xMax-xMin)/float64(numPoints-1))

	// Generate new points.
	var newPts plotter.XYs
	for i := 0; i < numPoints; i++ {
		newX := xMin + float64(i)*step
		pt := plotter.XY{X: newX, Y: interpolator.Predict(newX)}
		newPts = append(newPts, pt)
		if i > numPoints-3 {
			step2 := math.Max(0.0000001, (xMax-xMin)/float64(1000000))

			for j := 0; j < 3*numPoints; j++ {
				newX2 := newX + float64(j)*step2
				pt = plotter.XY{X: newX2, Y: interpolator.Predict(newX2)}
				newPts = append(newPts, pt)
			}
		}
	}

	return newPts
}
