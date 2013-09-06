package stackoverflow.lucene;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

public class NearestNeighbors {
	private final static ThreadLocal<MoreLikeThis> mltCache = new ThreadLocal<MoreLikeThis>();
	private static int numResults;
	
	public static class Searcher implements Runnable {
		
		private final String body;
		private int docId;
		
		public Searcher(int docId, String body) {
			this.docId = docId;
			this.body = body;
		}
		
		@Override
		public void run() {
			try {
				MoreLikeThis mlt = mltCache.get();
				if (mlt == null) {
					mlt = new MoreLikeThis(reader);
					mlt.setAnalyzer(wrapper);
					mlt.setFieldNames(new String[] {"body"});
				}
				Query query = mlt.like(new StringReader(body), "body");
				
				ScoreDoc[] hits = searcher.search(query, numResults).scoreDocs;
				
				StringBuilder result = new StringBuilder();
				result.append(docId);
				for (ScoreDoc hit : hits) {
					result.append(',');
					int topId = hit.doc;
					result.append(topId);
					result.append(",\"");
					Document doc = searcher.doc(topId, tagFields);
					String tags = doc.getField("tags").stringValue();
					result.append(tags);
					result.append('\"');
				}
				
				System.out.println(result);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static IndexSearcher searcher;
	private static PerFieldAnalyzerWrapper wrapper;
	private static DirectoryReader reader;
	private static Set<String> tagFields;
	
	public static void main(String[] args) throws Exception {
		Class.forName("org.sqlite.JDBC");
		Connection connection = DriverManager.getConnection(args[0]);

		Directory indexDir = new MMapDirectory(new File(args[1]));
		
		reader = DirectoryReader.open(indexDir);
		String q;
		numResults = Integer.parseInt(args[2]);
		if (args.length < 4) {
			q = "SELECT id, title, body FROM questions;";
		} else {
			q = "SELECT id, title, body FROM questions OFFSET " + args[3] + ";";
		}
		System.err.println(q);
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(q);

		HashMap<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();
		analyzerPerField.put("tags", new WhitespaceAnalyzer(Version.LUCENE_44));
		wrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_44), analyzerPerField);
		searcher = new IndexSearcher(reader);
		tagFields = Collections.singleton("tags");
		
		int numThreads = 8;
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<Object>(executor);
		
		int count = 0;
		while (result.next()) {
			count++;
			completionService.submit(new Searcher(result.getInt("id"), result.getString("body")), null);
			if (count > numThreads) {
				completionService.take().get();
			}
		}
		
		executor.shutdown();
		System.err.println("Waiting for searchers to finish...");
		while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
			System.err.println("Still waiting for samplers to finish...");
		}
		
		reader.close();
		result.close();
		statement.close();
		connection.close();
	}
}
