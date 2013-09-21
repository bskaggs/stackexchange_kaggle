package stackoverflow.lucene.mapreduce;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CachedSqliteInputFormat extends
		InputFormat<LongWritable, ResultSet> {
	public static class CSInputSplit extends InputSplit implements Writable {
		public String sql;

		public CSInputSplit() {
		}

		public CSInputSplit(String sql) {
			super();
			this.sql = sql;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 1;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(sql);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			sql = in.readUTF();
		}
	}
	
	public static void debugListFiles(Path[] paths) {
		for(Path path : paths) {
			debugListFiles(new File(path.getName()));
		}
	}
	public static void debugListFiles(File file) {
		boolean isDir = file.isDirectory();
		System.err.println(file + (isDir ? "/" : "\t" + file.length()));
		if (isDir) {
			for (File f : file.listFiles()) {
				debugListFiles(f);
			}
		}
	}

	public static class CSRecordReader extends
			RecordReader<LongWritable, ResultSet> {

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			String sql = ((CSInputSplit) split).sql;
			Configuration conf = context.getConfiguration();
			try {
				Class.forName("org.sqlite.JDBC");
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			
			System.err.println("archives:");
			Path[] archives = DistributedCache.getLocalCacheArchives(conf);
			if (archives != null) {
				debugListFiles(archives);
			}
			System.err.println("\nregular:");
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			if (cacheFiles != null) {
				debugListFiles(cacheFiles);
			}
			System.err.println("\ncurrdir:");
			debugListFiles(new File("."));
		
			
			try {
				File dbFile = new File(conf.get("sql.db"));
				if (dbFile.isDirectory()) {
					File[] files = dbFile.listFiles(new FilenameFilter() {
						@Override
						public boolean accept(File dir, String name) {
							return !name.startsWith(".") && name.endsWith(".db");
						}
					});
					
					for (File file : files) {
						dbFile = file;
						break;
					}
				}
				
				System.err.println("dbfile: " + dbFile);
				
				connection = DriverManager.getConnection("jdbc:sqlite:"
						+ dbFile.toString());
				statement = connection.createStatement();
				value = statement.executeQuery(sql);
				key = new LongWritable(-1);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		private LongWritable key;
		private ResultSet value;
		private Statement statement;
		private Connection connection;

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			try {
				if (!value.next()) {
					return false;
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
			key.set(key.get() + 1);
			return true;
		}

		@Override
		public LongWritable getCurrentKey() {
			return key;
		}

		@Override
		public ResultSet getCurrentValue() {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			try {
				if (value != null) {
					value.close();
				}
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}

	public final static Charset UTF8 = Charset.forName("UTF-8");

	@Override
	public RecordReader<LongWritable, ResultSet> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new CSRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Path sqlFile = new Path(conf.get("sql.file"));
		FileSystem fs = FileSystem.get(sqlFile.toUri(), conf);

		BufferedReader in = new BufferedReader(new InputStreamReader(
				fs.open(sqlFile), UTF8));
		String line;
		List<InputSplit> splits = new ArrayList<InputSplit>();
		while ((line = in.readLine()) != null) {
			splits.add(new CSInputSplit(line));
		}
		in.close();
		return splits;
	}
}
