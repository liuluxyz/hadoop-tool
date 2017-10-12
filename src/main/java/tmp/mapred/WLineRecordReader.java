/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tmp.mapred;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

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

/**
 * Treats keys as offset in file and value as line. 
 * @deprecated Use 
 *   {@link org.apache.hadoop.mapreduce.lib.input.LineRecordReader} instead.
 */
@Deprecated
public class WLineRecordReader implements RecordReader<LongWritable, Text> {
      private static final Log LOG
        = LogFactory.getLog(WLineRecordReader.class.getName());

      private CompressionCodecFactory compressionCodecs = null;
      private long start;
      private long pos;
      private long end;
      private LineReader in;
      int maxLineLength;

      /**
       * A class that provides a line reader from an input stream.
       * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
       */
      @Deprecated
      public static class LineReader{
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

      /**
       * Create a line reader that reads from the given stream using the
       * default buffer-size (64k).
       * @param in The input stream
       * @throws IOException
       */
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
        long bytesConsumed = 0;
        byte lastBy='\n';
        do {
          int startPosn = bufferPosn; //starting from where we left off the last time
          if (bufferPosn >= bufferLength) {
            startPosn = bufferPosn = 0;
            bufferLength = in.read(buffer);
            if (bufferLength <= 0)
              break; // EOF
          }
          for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
            if (buffer[bufferPosn] == LF && lastBy==CR) {
            	if(bufferPosn==0){
            		byte[] bs=java.util.Arrays.copyOf(str.getBytes(), str.getLength());
            		str.clear();
            		str.append(bs, 0, bs.length-1);
            	}
              newlineLength = 2;
              ++bufferPosn; // at next invocation proceed from following byte
              break;
            }
            lastBy=buffer[bufferPosn];
          }
          int readLength = bufferPosn - startPosn;
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

	//str.set(new String(java.util.Arrays.copyOf(str.getBytes(), str.getLength()), "GBK"));
        
        
	byte[] bs=java.util.Arrays.copyOf(str.getBytes(), str.getLength());
	str.clear();
	int start=0;
	int end=0;
	byte[] split={'\005'};byte oclSplit='\001';byte rp='\040';
	int len=bs.length;
	for(int i=0;i<len;i++){
	    if(bs[i]==oclSplit)bs[i]=rp;
	    else if(bs[i]==LF)bs[i]=rp;
	    else if(bs[i] == split[0]){
			if(end>start){
			    byte[] b=new String(java.util.Arrays.copyOfRange(bs, start, end), "GBK").getBytes("UTF-8");
			    str.append(b, 0, b.length);
			}
			str.append(split, 0, 1);
			start=end+1;
	    }
	    end++;
	}
	if(end>start){
	    byte[] b=new String(java.util.Arrays.copyOfRange(bs, start, end), "GBK").getBytes("UTF-8");
	    str.append(b, 0, b.length);
	}
    
        return (int)bytesConsumed;
      }

      public int readLine(Text str, int maxLineLength) throws IOException {
        return readLine(str, maxLineLength, Integer.MAX_VALUE);
    }

      public int readLine(Text str) throws IOException {
        return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
      }
  }

  public WLineRecordReader(Configuration job,
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
  
  public WLineRecordReader(InputStream in, long offset, long endOffset,
                          int maxLineLength) {
    this.maxLineLength = maxLineLength;
    this.in = new LineReader(in);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
  }

  public WLineRecordReader(InputStream in, long offset, long endOffset,
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
