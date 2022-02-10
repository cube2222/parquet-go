package rle

import (
	"fmt"
	"io"
	. "math/bits"
	"unsafe"

	"github.com/segmentio/parquet-go/internal/bits"
)

type bitPackRunDecoder struct {
	source   io.LimitedReader
	reader   bits.Reader
	remain   uint
	bitWidth uint
}

func (d *bitPackRunDecoder) String() string { return "BIT_PACK" }

func (d *bitPackRunDecoder) reset(r io.Reader, bitWidth, numValues uint) {
	d.source.R = r
	d.source.N = int64(bits.ByteCount(numValues * bitWidth))
	d.reader.Reset(&d.source)
	d.remain = numValues
	d.bitWidth = bitWidth
}

func (d *bitPackRunDecoder) decode(dst []byte, dstWidth uint) (n int, err error) {
	dstBitCount := bits.BitCount(len(dst))

	if dstWidth < 8 || dstWidth > 64 || OnesCount(dstWidth) != 1 {
		return 0, fmt.Errorf("BIT_PACK decoder expects the output size to be a power of 8 bits but got %d bits", dstWidth)
	}

	if (dstBitCount & (dstWidth - 1)) != 0 { // (dstBitCount % dstWidth) != 0
		return 0, fmt.Errorf("BIT_PACK decoder expects the input size to be a multiple of the destination width: bit-count=%d bit-width=%d",
			dstBitCount, dstWidth)
	}

	if dstWidth < d.bitWidth {
		return 0, fmt.Errorf("BIT_PACK decoder cannot encode %d bits values to %d bits: the source width must be less or equal to the destination width",
			d.bitWidth, dstWidth)
	}

	switch dstWidth {
	case 8:
		n, err = d.decodeInt8(bits.BytesToInt8(dst), d.bitWidth)
	case 16:
		n, err = d.decodeInt16(bits.BytesToInt16(dst), d.bitWidth)
	case 32:
		n, err = d.decodeInt32(bits.BytesToInt32(dst), d.bitWidth)
	case 64:
		n, err = d.decodeInt64(bits.BytesToInt64(dst), d.bitWidth)
	default:
		panic("BUG: unsupported destination bit-width")
	}

	if d.remain -= uint(n); d.remain == 0 {
		err = io.EOF
	} else if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}

	return n, err
}

func (d *bitPackRunDecoder) decodeInt8(dst []int8, bitWidth uint) (n int, err error) {
	for uint(n) < d.remain && n < len(dst) {
		b, _, err := d.reader.ReadBits(bitWidth)
		if err != nil {
			return int(n), err
		}
		dst[n] = int8(b)
		n++
	}
	return int(n), nil
}

func (d *bitPackRunDecoder) decodeInt16(dst []int16, bitWidth uint) (n int, err error) {
	for uint(n) < d.remain && n < len(dst) {
		b, _, err := d.reader.ReadBits(bitWidth)
		if err != nil {
			return n, err
		}
		dst[n] = int16(b)
		n++
	}
	return n, nil
}

func (d *bitPackRunDecoder) decodeInt32(dst []int32, bitWidth uint) (n int, err error) {
	for uint(n) < d.remain && n < len(dst) {
		b, _, err := d.reader.ReadBits(bitWidth)
		if err != nil {
			return n, err
		}
		dst[n] = int32(b)
		n++
	}
	return n, nil
}

func (d *bitPackRunDecoder) decodeInt64(dst []int64, bitWidth uint) (n int, err error) {
	for uint(n) < d.remain && n < len(dst) {
		b, _, err := d.reader.ReadBits(bitWidth)
		if err != nil {
			return n, err
		}
		dst[n] = int64(b)
		n++
	}
	return n, nil
}

type bitPackRunEncoder struct {
	writer   bits.Writer
	bitWidth uint
}

func (e *bitPackRunEncoder) reset(w io.Writer, bitWidth uint) {
	e.writer.Reset(w)
	e.bitWidth = bitWidth
}

func (e *bitPackRunEncoder) encode(src []byte, srcWidth uint) error {
	srcBitCount := bits.BitCount(len(src))

	if srcWidth < 8 || srcWidth > 64 || OnesCount(srcWidth) != 1 {
		return fmt.Errorf("BIT_PACK encoder expects the input size to be a power of 8 bits but got %d bits", srcWidth)
	}

	if (srcBitCount & (srcWidth - 1)) != 0 { // (srcBitCount % srcWidth) != 0
		return fmt.Errorf("BIT_PACK encoder expects the input size to be a multiple of the source width: bit-count=%d bit-width=%d", srcBitCount, srcWidth)
	}

	if ((srcBitCount / srcWidth) % 8) != 0 {
		return fmt.Errorf("BIT_PACK encoder expects sequences of 8 values but %d were written", srcBitCount/srcWidth)
	}

	if srcWidth < e.bitWidth {
		return fmt.Errorf("BIT_PACK encoder cannot encode %d bits values to %d bits: the source width must be less or equal to the destination width",
			srcWidth, e.bitWidth)
	}

	var err error
	switch srcWidth {
	case 8:
		err = e.writer.WriteInt8x8(bytesToInt8x8(src), e.bitWidth)
	case 16:
		err = e.writer.WriteInt16x8(bytesToInt16x8(src), e.bitWidth)
	case 32:
		err = e.writer.WriteInt32x8(bytesToInt32x8(src), e.bitWidth)
	case 64:
		err = e.writer.WriteInt64x8(bytesToInt64x8(src), e.bitWidth)
	default:
		panic("BUG: unsupported source bit-width")
	}
	if err != nil {
		return fmt.Errorf("BIT_PACK encoding %d bits values to %d bits: %w", srcWidth, e.bitWidth, err)
	}
	return nil
}

func bytesToInt8x8(data []byte) [][8]int8 {
	return unsafe.Slice(*(**[8]int8)(unsafe.Pointer(&data)), len(data)/8)
}

func bytesToInt16x8(data []byte) [][8]int16 {
	return unsafe.Slice(*(**[8]int16)(unsafe.Pointer(&data)), len(data)/16)
}

func bytesToInt32x8(data []byte) [][8]int32 {
	return unsafe.Slice(*(**[8]int32)(unsafe.Pointer(&data)), len(data)/32)
}

func bytesToInt64x8(data []byte) [][8]int64 {
	return unsafe.Slice(*(**[8]int64)(unsafe.Pointer(&data)), len(data)/64)
}
