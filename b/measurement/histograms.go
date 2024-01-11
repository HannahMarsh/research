package measurement

//import (
//	bconfig "benchmark/config"
//	"bufio"
//	"fmt"
//	"io"
//	"os"
//	"sort"
//	"time"
//
//	"github.com/pingcap/go-ycsb/pkg/util"
//)
//
//type Histograms struct {
//	p *bconfig.Config
//
//	histograms map[string]*histogram
//}
//
//func (h *Histograms) GenerateExtendedOutputs() {
//	if h.p.Measurements.HistogramPercentilesExport.Value {
//		for op, opM := range h.histograms {
//			outFile := fmt.Sprintf("%s%s-percentiles.txt", h.p.Measurements.HistogramOutputDir, op)
//			fmt.Printf("Exporting the full latency spectrum for operation '%s' in percentile output format into file: %s.\n", op, outFile)
//			f, err := os.Create(outFile)
//			if err != nil {
//				panic("failed to create percentile output file: " + err.Error())
//			}
//			defer func(f *os.File) {
//				err := f.Close()
//				if err != nil {
//					panic("failed to close percentile output file: " + err.Error())
//				}
//			}(f)
//			w := bufio.NewWriter(f)
//			_, err = opM.hist.PercentilesPrint(w, 1, 1.0)
//			err = w.Flush()
//			if err != nil {
//				panic(err)
//			}
//			if err != nil {
//				panic("failed to print percentiles: " + err.Error())
//			}
//		}
//	}
//}
//
//func (h *Histograms) Measure(op string, start time.Time, lan time.Duration) {
//	opM, ok := h.histograms[op]
//	if !ok {
//		opM = newHistogram()
//		h.histograms[op] = opM
//	}
//
//	opM.Measure(lan)
//}
//
//func (h *Histograms) summary() map[string][]string {
//	summaries := make(map[string][]string, len(h.histograms))
//	for op, opM := range h.histograms {
//		summaries[op] = opM.Summary()
//	}
//	return summaries
//}
//
//func (h *Histograms) Summary() {
//	err := h.Output(os.Stdout)
//	if err != nil {
//		panic(err)
//	}
//}
//
//func (h *Histograms) Output(w io.Writer) error {
//	summaries := h.summary()
//	keys := make([]string, 0, len(summaries))
//	for k := range summaries {
//		keys = append(keys, k)
//	}
//	sort.Strings(keys)
//
//	lines := [][]string{}
//	for _, op := range keys {
//		line := []string{op}
//		line = append(line, summaries[op]...)
//		lines = append(lines, line)
//	}
//
//	outputStyle := h.p.Measurements.OutputStyle.Value
//	switch outputStyle {
//	case util.OutputStylePlain:
//		util.RenderString(w, "%-6s - %s\n", header, lines)
//	case util.OutputStyleJson:
//		util.RenderJson(w, header, lines)
//	case util.OutputStyleTable:
//		util.RenderTable(w, header, lines)
//	default:
//		panic("unsupported outputstyle: " + outputStyle)
//	}
//	return nil
//}
//
//func InitHistograms(p *bconfig.Config) *Histograms {
//	return &Histograms{
//		p:          p,
//		histograms: make(map[string]*histogram, 16),
//	}
//}
