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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.MMapDirectory;

import stackoverflow.lucene.modified.BM25Similarity;
import stackoverflow.lucene.modified.MoreLikeThis;

public class NearestNeighborsRunner {
	private final static ThreadLocal<NearestNeighborsFinder> finderCache = new ThreadLocal<NearestNeighborsFinder>();
	private static int numResults;
	
	private static Term corpusTerm;
	
	private static IndexSearcher searcher;
	private static Analyzer analyzer;
	
	private static DirectoryReader termStatsReader;
	private static DirectoryReader searchReader;
	
	public static NearestNeighborsFinder getFinder() {
		NearestNeighborsFinder finder = finderCache.get();
		if (finder == null) {
			MoreLikeThis mlt = new MoreLikeThis(searchReader, termStatsReader);
			mlt.setAnalyzer(analyzer);
			String searchField = "text";
			mlt.setFieldNames(new String[] {searchField} );
			mlt.setMinDocFreq(1);
			mlt.setMinTermFreq(1);
			
			finder = new NearestNeighborsFinder(mlt, searcher, searchField, corpusTerm, numResults);
			finderCache.set(finder);
		}
		return finder;
	}
	
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption("d", true, "database");
		options.addOption("i", true, "index directory (training and testing)");
		options.addOption("t", true, "term stats directory (training only)");
		options.addOption("a", true, "analyzer class. Default is standardanalyzer");
		options.addOption("s", true, "SQL to run");
		options.addOption("c", true, "Corpus name");
		options.addOption("n", true, "Number of results");
		options.addOption("h", "help", false, "Get help");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		
		if (cmd.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "indexer", options );
			System.exit(0);
		}
		
		
		Class.forName("org.sqlite.JDBC");
		Connection connection = DriverManager.getConnection(cmd.getOptionValue("d"));
		searchReader = DirectoryReader.open(new MMapDirectory(new File(cmd.getOptionValue("i"))));
		
		String termStatsDir = cmd.getOptionValue("m");
		if (termStatsDir != null) {
			termStatsReader = DirectoryReader.open(new MMapDirectory(new File(termStatsDir)));
		} else {
			termStatsReader = searchReader;
		}
		
		searcher = new IndexSearcher(searchReader);
		searcher.setSimilarity(new BM25Similarity());
		
		numResults = Integer.parseInt(cmd.getOptionValue("n"));
		String fieldSql = cmd.getOptionValue("s");
		String corpus = cmd.getOptionValue("c");
		if (corpus != null) {
			 corpusTerm = new Term("corpus", corpus);
		}
		
		String q = "SELECT id, " + fieldSql + " as text FROM questions;";
		
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(q);
		
		analyzer = Indexer.getAnalyzer(cmd.getOptionValue("a"));
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
					return getFinder().find(id, text);
				}
			});
			
			if (count > numThreads) {
				String res = completionService.take().get();
				System.out.println(res);
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
		
		termStatsReader.close();
		if (termStatsReader != searchReader) {
			searchReader.close();
		}
		result.close();
		statement.close();
		connection.close();
	}
}
