package org.example.medicaments.types;


import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;

import java.io.IOException;

@Slf4j
@NoArgsConstructor
public class MedicamentLineRecordReader extends RecordReader<MedicamentLongWritable, MedicamentText> {
    public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";
    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private MedicamentLongWritable key;
    private MedicamentText value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;


    public MedicamentLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit)genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        FutureDataInputStreamBuilder builder = file.getFileSystem(job).openFile(file);
        FutureIOSupport.propagateOptions(builder, job, "mapreduce.job.input.file.option.", "mapreduce.job.input.file.must.");
        this.fileIn = (FSDataInputStream)FutureIOSupport.awaitFuture(builder.build());
        CompressionCodec codec = (new CompressionCodecFactory(job)).getCodec(file);
        if (null != codec) {
            this.isCompressedInput = true;
            this.decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new CompressedSplitLineReader(cIn, job, this.recordDelimiterBytes);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                if (this.start != 0L) {
                    throw new IOException("Cannot seek in " + codec.getClass().getSimpleName() + " compressed stream");
                }

                this.in = new SplitLineReader(codec.createInputStream(this.fileIn, this.decompressor), job, this.recordDelimiterBytes);
                this.filePosition = this.fileIn;
            }
        } else {
            this.fileIn.seek(this.start);
            this.in = new UncompressedSplitLineReader(this.fileIn, job, this.recordDelimiterBytes, split.getLength());
            this.filePosition = this.fileIn;
        }

        if (this.start != 0L) {
            this.start += (long)this.in.readLine(new MedicamentText(), 0, this.maxBytesToConsume(this.start));
        }

        this.pos = this.start;
    }

    private int maxBytesToConsume(long pos) {
        return this.isCompressedInput ? Integer.MAX_VALUE : (int)Math.max(Math.min(2147483647L, this.end - pos), (long)this.maxLineLength);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (this.isCompressedInput && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }

        return retVal;
    }

    private int skipUtfByteOrderMark() throws IOException {
        int newMaxLineLength = (int)Math.min(3L + (long)this.maxLineLength, 2147483647L);
        int newSize = this.in.readLine(this.value, newMaxLineLength, this.maxBytesToConsume(this.pos));
        this.pos += (long)newSize;
        int textLength = this.value.getLength();
        byte[] textBytes = this.value.getBytes();
        if (textLength >= 3 && textBytes[0] == -17 && textBytes[1] == -69 && textBytes[2] == -65) {
            log.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                textBytes = this.value.copyBytes();
                this.value.set(textBytes, 3, textLength);
            } else {
                this.value.clear();
            }
        }

        return newSize;
    }

    public boolean nextKeyValue() throws IOException {
        if (this.key == null) {
            this.key = new MedicamentLongWritable();
        }

        this.key.set(this.pos);
        if (this.value == null) {
            this.value = new MedicamentText();
        }

        int newSize = 0;

        while(this.getFilePosition() <= this.end || this.in.needAdditionalRecordAfterSplit()) {
            if (this.pos == 0L) {
                newSize = this.skipUtfByteOrderMark();
            } else {
                newSize = this.in.readLine(this.value, this.maxLineLength, this.maxBytesToConsume(this.pos));
                this.pos += (long)newSize;
            }

            if (newSize == 0 || newSize < this.maxLineLength) {
                break;
            }

            log.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
        }

        if (newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }

    public MedicamentLongWritable getCurrentKey() {
        return this.key;
    }

    public MedicamentText getCurrentValue() {
        return this.value;
    }

    public float getProgress() throws IOException {
        return this.start == this.end ? 0.0F : Math.min(1.0F, (float)(this.getFilePosition() - this.start) / (float)(this.end - this.start));
    }

    public synchronized void close() throws IOException {
        try {
            if (this.in != null) {
                this.in.close();
            }
        } finally {
            if (this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
                this.decompressor = null;
            }

        }

    }
}