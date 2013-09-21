package stackoverflow.lucene.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

import stackoverflow.lucene.NearestNeighborsFinder;

public class HadoopNearestNeighborsFinder extends Configured implements Tool{
	public static class NearestNeighborsMapper extends Mapper<LongWritable, ResultSet, Text, NullWritable> {
		private NearestNeighborsFinder finder;
		private DirectoryReader reader;
		protected void setup(Context context) throws java.io.IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			int numResults = conf.getInt("nnf.number", 5);
			Query corpusQuery = new TermQuery(new Term("corpus", "train"));
			reader = DirectoryReader.open(new MMapDirectory(new File(conf.get("nnf.index") + "/" + conf.get("nnf.index") )));
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_44);
			IndexSearcher searcher = new IndexSearcher(reader);
			MoreLikeThis mlt = new MoreLikeThis(reader);
			mlt.setAnalyzer(analyzer);
			String searchField = "text";
			mlt.setFieldNames(new String[] {searchField} );
			
			finder = new NearestNeighborsFinder(mlt, searcher, searchField, corpusQuery, numResults);
		}
		
		protected void map(LongWritable key, ResultSet result, Context context) throws java.io.IOException, InterruptedException {
			try {
				String id = result.getString("id");
				String text = result.getString("text");
				context.write(new Text(finder.find(id, text)), NullWritable.get());
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			reader.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Nearest Neighbor Finder");
		Configuration conf = job.getConfiguration();
		DistributedCache.createSymlink(conf);
		job.setJarByClass(getClass());
		job.setInputFormatClass(CachedSqliteInputFormat.class);
		URI db = new URI(args[0]);
		DistributedCache.addCacheArchive(db, conf);
		conf.set("sql.db", db.getFragment());
		conf.set("sql.file", args[1]);
		
		job.setMapperClass(NearestNeighborsMapper.class);
		URI indexDir = new URI(args[2]);
		DistributedCache.addCacheArchive(indexDir, conf);
		conf.set("nnf.index", indexDir.getFragment());
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[3]));
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new HadoopNearestNeighborsFinder(), args));
	}
}
