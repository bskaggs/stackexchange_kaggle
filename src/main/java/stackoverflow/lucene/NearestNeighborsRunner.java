package stackoverflow.lucene;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

public class NearestNeighborsRunner {
	private final static ThreadLocal<NearestNeighborsFinder> finderCache = new ThreadLocal<NearestNeighborsFinder>();
	private static int numResults;
	
	private static TermQuery corpusQuery;
	
	private static IndexSearcher searcher;
	private static Analyzer analyzer;
	private static DirectoryReader reader;
	
	public static NearestNeighborsFinder getFinder() {
		NearestNeighborsFinder finder = finderCache.get();
		if (finder == null) {
			MoreLikeThis mlt = new MoreLikeThis(reader);
			mlt.setAnalyzer(analyzer);
			String searchField = "text";
			mlt.setFieldNames(new String[] {searchField} );
			finder = new NearestNeighborsFinder(mlt, searcher, searchField, corpusQuery, numResults);
			finderCache.set(finder);
		}
		return finder;
	}
	
	public static void main(String[] args) throws Exception {
		Class.forName("org.sqlite.JDBC");
	
		Connection connection = DriverManager.getConnection(args[0]);
		reader = DirectoryReader.open(new MMapDirectory(new File(args[1])));
		numResults = Integer.parseInt(args[2]);
		String fieldSql = args[3];
		if (args.length > 4 && !("-".equals(args[4]))) {
			 corpusQuery = new TermQuery(new Term("corpus", args[4]));
		}
		
		String q = "SELECT id, " + fieldSql + " as text FROM questions;";
		
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(q);

		analyzer = new StandardAnalyzer(Version.LUCENE_44);
		searcher = new IndexSearcher(reader);
		
		int numThreads = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		ExecutorCompletionService<String> completionService = new ExecutorCompletionService<String>(executor);
		
		int count = 0;
		while (result.next()) {
			count++;
			
		
			final String id = result.getString("id");
			final String text = result.getString("text");
			
			completionService.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					NearestNeighborsFinder finder = getFinder();
					return finder.find(id, text);
				}
			});
			
			if (count > numThreads) {
				System.out.println(completionService.take().get());
			}
			
			if (count % 10000 == 0) {
				System.err.println(count);
			}
		}
		
		executor.shutdown();
		System.err.println("Waiting for searchers to finish...");
		while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
			System.err.println("Still waiting for samplers to finish...");
		}
		
		//print out any remaining tasks
		Future<String> task;
		while((task = completionService.poll()) != null) {
			System.out.println(task.get());
		}
		
		reader.close();
		result.close();
		statement.close();
		connection.close();
	}
}
