package main

import (
	"bufio"
	"container/heap"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type CampaignAgg struct {
	TotalImpressions int64
	TotalClicks      int64
	TotalSpend       float64
	TotalConversions int64
}

type CampaignMetrics struct {
	CampaignID       string
	TotalImpressions int64
	TotalClicks      int64
	TotalSpend       float64
	TotalConversions int64
	CTR              float64
	CPA              float64
	HasCPA           bool
}

type CTRHeap []CampaignMetrics

func (h CTRHeap) Len() int { return len(h) }

func (h CTRHeap) Less(i, j int) bool {
	if h[i].CTR != h[j].CTR {
		return h[i].CTR < h[j].CTR
	}
	return h[i].CampaignID > h[j].CampaignID
}

func (h CTRHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *CTRHeap) Push(x any) {
	*h = append(*h, x.(CampaignMetrics))
}

func (h *CTRHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type CPAHeap []CampaignMetrics

func (h CPAHeap) Len() int { return len(h) }

func (h CPAHeap) Less(i, j int) bool {
	if h[i].CPA != h[j].CPA {
		return h[i].CPA > h[j].CPA
	}
	return h[i].CampaignID > h[j].CampaignID
}

func (h CPAHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *CPAHeap) Push(x any) {
	*h = append(*h, x.(CampaignMetrics))
}

func (h *CPAHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func buildMetrics(campaignID string, agg *CampaignAgg) CampaignMetrics {
	ctr := 0.0
	if agg.TotalImpressions > 0 {
		ctr = float64(agg.TotalClicks) / float64(agg.TotalImpressions)
	}

	metrics := CampaignMetrics{
		CampaignID:       campaignID,
		TotalImpressions: agg.TotalImpressions,
		TotalClicks:      agg.TotalClicks,
		TotalSpend:       agg.TotalSpend,
		TotalConversions: agg.TotalConversions,
		CTR:              ctr,
	}

	if agg.TotalConversions > 0 {
		metrics.CPA = agg.TotalSpend / float64(agg.TotalConversions)
		metrics.HasCPA = true
	}

	return metrics
}

func betterCTR(a, b CampaignMetrics) bool {
	if a.CTR != b.CTR {
		return a.CTR > b.CTR
	}
	return a.CampaignID < b.CampaignID
}

func betterCPA(a, b CampaignMetrics) bool {
	if a.CPA != b.CPA {
		return a.CPA < b.CPA
	}
	return a.CampaignID < b.CampaignID
}

func aggregateCampaigns(inputPath string) (map[string]*CampaignAgg, int64, error) {
	f, err := os.Open(inputPath)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReaderSize(f, 1024*1024))
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read CSV header: %w", err)
	}

	idx := make(map[string]int, len(header))
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}

	required := []string{"campaign_id", "impressions", "clicks", "spend", "conversions"}
	for _, col := range required {
		if _, ok := idx[col]; !ok {
			return nil, 0, fmt.Errorf("missing required column: %s", col)
		}
	}

	campaignIdx := idx["campaign_id"]
	impressionIdx := idx["impressions"]
	clickIdx := idx["clicks"]
	spendIdx := idx["spend"]
	conversionIdx := idx["conversions"]

	aggs := make(map[string]*CampaignAgg)
	var processed int64

	for {
		record, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			if parseErr, ok := readErr.(*csv.ParseError); ok && parseErr.Err == csv.ErrFieldCount {
				continue
			}
			return nil, 0, fmt.Errorf("failed while reading CSV: %w", readErr)
		}
		if len(record) == 0 {
			continue
		}

		campaignID := strings.TrimSpace(record[campaignIdx])
		impressions, err1 := strconv.ParseInt(record[impressionIdx], 10, 64)
		clicks, err2 := strconv.ParseInt(record[clickIdx], 10, 64)
		spend, err3 := strconv.ParseFloat(record[spendIdx], 64)
		conversions, err4 := strconv.ParseInt(record[conversionIdx], 10, 64)

		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			continue
		}

		agg := aggs[campaignID]
		if agg == nil {
			agg = &CampaignAgg{}
			aggs[campaignID] = agg
		}

		agg.TotalImpressions += impressions
		agg.TotalClicks += clicks
		agg.TotalSpend += spend
		agg.TotalConversions += conversions
		processed++
	}

	return aggs, processed, nil
}

func selectTopCTR(aggs map[string]*CampaignAgg, k int) []CampaignMetrics {
	h := &CTRHeap{}
	heap.Init(h)

	for campaignID, agg := range aggs {
		m := buildMetrics(campaignID, agg)
		if h.Len() < k {
			heap.Push(h, m)
			continue
		}

		if betterCTR(m, (*h)[0]) {
			heap.Pop(h)
			heap.Push(h, m)
		}
	}

	result := make([]CampaignMetrics, h.Len())
	copy(result, *h)

	sort.Slice(result, func(i, j int) bool {
		if result[i].CTR != result[j].CTR {
			return result[i].CTR > result[j].CTR
		}
		return result[i].CampaignID < result[j].CampaignID
	})

	return result
}

func selectTopCPA(aggs map[string]*CampaignAgg, k int) []CampaignMetrics {
	h := &CPAHeap{}
	heap.Init(h)

	for campaignID, agg := range aggs {
		if agg.TotalConversions <= 0 {
			continue
		}

		m := buildMetrics(campaignID, agg)
		if h.Len() < k {
			heap.Push(h, m)
			continue
		}

		if betterCPA(m, (*h)[0]) {
			heap.Pop(h)
			heap.Push(h, m)
		}
	}

	result := make([]CampaignMetrics, h.Len())
	copy(result, *h)

	sort.Slice(result, func(i, j int) bool {
		if result[i].CPA != result[j].CPA {
			return result[i].CPA < result[j].CPA
		}
		return result[i].CampaignID < result[j].CampaignID
	})

	return result
}

func writeCSV(path string, rows []CampaignMetrics) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"campaign_id",
		"total_impressions",
		"total_clicks",
		"total_spend",
		"total_conversions",
		"CTR",
		"CPA",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, row := range rows {
		cpa := ""
		if row.HasCPA {
			cpa = fmt.Sprintf("%.6f", row.CPA)
		}

		rec := []string{
			row.CampaignID,
			strconv.FormatInt(row.TotalImpressions, 10),
			strconv.FormatInt(row.TotalClicks, 10),
			fmt.Sprintf("%.2f", row.TotalSpend),
			strconv.FormatInt(row.TotalConversions, 10),
			fmt.Sprintf("%.6f", row.CTR),
			cpa,
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}

	return w.Error()
}

func main() {
	input := flag.String("input", "", "Path to input CSV file")
	output := flag.String("output", "", "Output directory for result CSV files")
	topK := flag.Int("top-k", 10, "Top K campaigns")
	flag.Parse()

	if *input == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "usage: go run aggregator.go --input ad_data.csv --output results/")
		os.Exit(1)
	}
	if *topK <= 0 {
		fmt.Fprintln(os.Stderr, "--top-k must be > 0")
		os.Exit(1)
	}

	start := time.Now()

	aggs, processedRows, err := aggregateCampaigns(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aggregation failed: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*output, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	topCTR := selectTopCTR(aggs, *topK)
	topCPA := selectTopCPA(aggs, *topK)

	ctrPath := filepath.Join(*output, "top10_ctr.csv")
	cpaPath := filepath.Join(*output, "top10_cpa.csv")

	if err := writeCSV(ctrPath, topCTR); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write CTR output: %v\n", err)
		os.Exit(1)
	}
	if err := writeCSV(cpaPath, topCPA); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write CPA output: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	fmt.Printf("Processed rows: %d\n", processedRows)
	fmt.Printf("Distinct campaigns: %d\n", len(aggs))
	fmt.Printf("Top CTR output: %s\n", ctrPath)
	fmt.Printf("Top CPA output: %s\n", cpaPath)
	fmt.Printf("Elapsed time: %s\n", elapsed)
}
