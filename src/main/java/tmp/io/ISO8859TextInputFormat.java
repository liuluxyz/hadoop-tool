package tmp.io;

import java.io.IOException;

import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class ISO8859TextInputFormat implements
		InputFormat<LongWritable, BytesWritable>, JobConfigurable {

	public static class CustomLineRecordReader implements
			RecordReader<LongWritable, BytesWritable>, JobConfigurable {

		LineRecordReader reader;
		Text text;

		public CustomLineRecordReader(LineRecordReader reader) {
			this.reader = reader;
			text = reader.createValue();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public LongWritable createKey() {
			return reader.createKey();
		}

		@Override
		public BytesWritable createValue() {
			return new BytesWritable();
		}

		@Override
		public long getPos() throws IOException {
			return reader.getPos();
		}

		@Override
		public float getProgress() throws IOException {
			return reader.getProgress();
		}

		@Override
		public boolean next(LongWritable key, BytesWritable value)
				throws IOException {
			while (reader.next(key, text)) {
				String newText = new String(text.getBytes(), "ISO8859-1");
				byte[] array = newText.getBytes("UTF-8");
				value.set(array, 0, array.length);
				return true;
			}
			// no more data
			return false;
		}

		private final StringBuffer buf = new StringBuffer();

		@Override
		public void configure(JobConf job) {
		}

	}

	TextInputFormat format;
	JobConf job;

	public ISO8859TextInputFormat() {
		format = new TextInputFormat();
	}

	@Override
	public void configure(JobConf job) {
		this.job = job;
		format.configure(job);
	}

	public RecordReader<LongWritable, BytesWritable> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		reporter.setStatus(genericSplit.toString());
		CustomLineRecordReader reader = new CustomLineRecordReader(
				new LineRecordReader(job, (FileSplit) genericSplit));
		reader.configure(job);
		return reader;
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		return format.getSplits(job, numSplits);
	}

	// Cannot put @Override here because hadoop 0.18+ removed this method.
	public void validateInput(JobConf job) throws IOException {
//		ShimLoader.getHadoopShims().inputFormatValidateInput(format, job);
	}

}