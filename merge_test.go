package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
)

const (
	numRowGroups = 3
	rowsPerGroup = benchmarkReaderNumRows
)

func BenchmarkMergeRowGroups(b *testing.B) {
	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			schema := parquet.SchemaOf(test.model)

			options := []parquet.RowGroupOption{
				parquet.SortingColumns(
					parquet.Ascending(schema.Columns()[0]...),
				),
			}

			prng := rand.New(rand.NewSource(0))
			rowGroups := make([]parquet.RowGroup, numRowGroups)

			for i := range rowGroups {
				rowGroups[i] = sortedRowGroup(options, randomRowsOf(prng, rowsPerGroup, test.model)...)
			}

			for n := 1; n <= numRowGroups; n++ {
				b.Run(fmt.Sprintf("groups=%d,rows=%d", n, n*rowsPerGroup), func(b *testing.B) {
					mergedRowGroup, err := parquet.MergeRowGroups(rowGroups[:n])
					if err != nil {
						b.Fatal(err)
					}
					start := time.Now()

					rows := mergedRowGroup.Rows()
					rbuf := make(parquet.Row, 0, 16)

					for i := 0; i < b.N; i++ {
						rbuf, err = rows.ReadRow(rbuf[:0])
						if err != nil {
							if !errors.Is(err, io.EOF) {
								b.Fatal(err)
							}
							rows = mergedRowGroup.Rows()
						}
					}

					seconds := time.Since(start).Seconds()
					b.ReportMetric(float64(b.N)/seconds, "row/s")
				})
			}
		})
	}
}

func BenchmarkMergeFiles(b *testing.B) {
	rowGroupBuffers := make([]bytes.Buffer, numRowGroups)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			schema := parquet.SchemaOf(test.model)

			buffer := parquet.NewBuffer(
				schema,
				parquet.SortingColumns(
					parquet.Ascending(schema.Columns()[0]...),
				),
			)

			prng := rand.New(rand.NewSource(0))
			files := make([]*parquet.File, numRowGroups)
			rowGroups := make([]parquet.RowGroup, numRowGroups)

			for i := range files {
				for _, row := range randomRowsOf(prng, rowsPerGroup, test.model) {
					buffer.Write(row)
				}
				sort.Sort(buffer)
				rowGroupBuffers[i].Reset()
				writer := parquet.NewWriter(&rowGroupBuffers[i])
				_, err := parquet.CopyRows(writer, buffer.Rows())
				if err != nil {
					b.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
				r := bytes.NewReader(rowGroupBuffers[i].Bytes())
				f, err := parquet.OpenFile(r, r.Size())
				if err != nil {
					b.Fatal(err)
				}
				files[i], rowGroups[i] = f, f.RowGroups()[0]
			}

			for n := 1; n <= numRowGroups; n++ {
				b.Run(fmt.Sprintf("groups=%d,rows=%d", n, n*rowsPerGroup), func(b *testing.B) {
					mergedRowGroup, err := parquet.MergeRowGroups(rowGroups[:n])
					if err != nil {
						b.Fatal(err)
					}
					start := time.Now()

					rows := mergedRowGroup.Rows()
					rbuf := make(parquet.Row, 0, 16)

					for i := 0; i < b.N; i++ {
						rbuf, err = rows.ReadRow(rbuf[:0])
						if err != nil {
							if !errors.Is(err, io.EOF) {
								b.Fatal(err)
							}
							rows = mergedRowGroup.Rows()
						}
					}

					totalSize := int64(0)
					for _, f := range files[:n] {
						totalSize += f.Size()
					}

					seconds := time.Since(start).Seconds()
					b.ReportMetric(float64(b.N)/seconds, "row/s")
					b.SetBytes(int64(math.Ceil(float64(totalSize) / benchmarkReaderNumRows)))
				})
			}
		})
	}
}
