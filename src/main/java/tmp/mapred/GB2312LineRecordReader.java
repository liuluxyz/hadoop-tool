package tmp.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class GB2312LineRecordReader implements RecordReader<LongWritable, Text> {
  private static final Log LOG
    = LogFactory.getLog(GB2312LineRecordReader.class.getName());

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  int maxLineLength;

  public static class LineReader {
	  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	  private int bufferSize = DEFAULT_BUFFER_SIZE;
	  private InputStream in;
	  private byte[] buffer;
	  // the number of bytes of real data in the buffer
	  private int bufferLength = 0;
	  // the current position in the buffer
	  private int bufferPosn = 0;

	  private static final byte CR = '\r';
	  private static final byte LF = '\n';

	  public LineReader(InputStream in) {
	    this(in, DEFAULT_BUFFER_SIZE);
	  }

	  public LineReader(InputStream in, int bufferSize) {
	    this.in = in;
	    this.bufferSize = bufferSize;
	    this.buffer = new byte[this.bufferSize];
	  }

	  public LineReader(InputStream in, Configuration conf) throws IOException {
	    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
	  }

	  public void close() throws IOException {
	    in.close();
	  }
	  
	  public int readLine(Text str, int maxLineLength,
	                      int maxBytesToConsume) throws IOException {
	    str.clear();
	    int txtLength = 0; //tracks str.getLength(), as an optimization
	    int newlineLength = 0; //length of terminating newline
	    boolean prevCharCR = false; //true of prev char was CR
	    long bytesConsumed = 0;
	    do {
	      int startPosn = bufferPosn; //starting from where we left off the last time
	      if (bufferPosn >= bufferLength) {
	        startPosn = bufferPosn = 0;
	        if (prevCharCR)
	          ++bytesConsumed; //account for CR from previous read
	        bufferLength = in.read(buffer);
	        if (bufferLength <= 0)
	          break; // EOF
	      }
	      for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
	        if (buffer[bufferPosn] == LF) {
	          newlineLength = (prevCharCR) ? 2 : 1;
	          ++bufferPosn; // at next invocation proceed from following byte
	          break;
	        }
	        if (prevCharCR) { //CR + notLF, we are at notLF
	          newlineLength = 1;
	          break;
	        }
	        prevCharCR = (buffer[bufferPosn] == CR);
	      }
	      int readLength = bufferPosn - startPosn;
	      if (prevCharCR && newlineLength == 0)
	        --readLength; //CR at the end of the buffer
	      bytesConsumed += readLength;
	      int appendLength = readLength - newlineLength;
	      if (appendLength > maxLineLength - txtLength) {
	        appendLength = maxLineLength - txtLength;
	      }
	      if (appendLength > 0) {
	        str.append(buffer, startPosn, appendLength);
	        txtLength += appendLength;
	      }
	    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

	    if (bytesConsumed > (long)Integer.MAX_VALUE)
	      throw new IOException("Too many bytes before newline: " + bytesConsumed);    
	    
	    //encoding
	    str.set(new String(java.util.Arrays.copyOf(str.getBytes(), str.getLength()), "GB2312"));
	    
	    
	    return (int)bytesConsumed;
	  }

	  public int readLine(Text str, int maxLineLength) throws IOException {
	    return readLine(str, maxLineLength, Integer.MAX_VALUE);
	}

	  public int readLine(Text str) throws IOException {
	    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
	  }

	}

  public GB2312LineRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  public GB2312LineRecordReader(InputStream in, long offset, long endOffset,
                          int maxLineLength) {
    this.maxLineLength = maxLineLength;
    this.in = new LineReader(in);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
  }

  public GB2312LineRecordReader(InputStream in, long offset, long endOffset, 
                          Configuration job) 
    throws IOException{
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    this.in = new LineReader(in, job);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  public Text createValue() {
    return new Text();
  }
  
  /** Read a line. */
  public synchronized boolean next(LongWritable key, Text value)
    throws IOException {

    while (pos < end) {
      key.set(pos);

      int newSize = in.readLine(value, maxLineLength,
                                Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                         maxLineLength));
      if (newSize == 0) {
        return false;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        return true;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    return false;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}
