package stackoverflow.lucene;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class Index {
	public static void main(String[] args) throws Exception {
		Class.forName("org.sqlite.JDBC");
		Connection connection = DriverManager.getConnection(args[0]);
		
		HashMap<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();
		analyzerPerField.put("tags", new WhitespaceAnalyzer(Version.LUCENE_44));
		PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_44), analyzerPerField);
		
		Directory indexDir = new SimpleFSDirectory(new File(args[1]));
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_44, wrapper);
		final IndexWriter writer = new IndexWriter(indexDir, config);

		String query = "SELECT id, title, body, code, links, tags FROM questions;";
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(query);

		int count = 0;
		int numThreads = 8;
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<Object>(executor);
		while (result.next()) {
			count ++;
			final Document document = new Document();
			document.add(new IntField("id", result.getInt("id"), Field.Store.YES));
			document.add(new TextField("title", result.getString("title"),	Field.Store.NO));
			document.add(new TextField("body", result.getString("body"), Field.Store.NO));
			document.add(new TextField("code", result.getString("code"), Field.Store.NO));
			document.add(new TextField("links", result.getString("links"), Field.Store.NO));
			document.add(new TextField("tags", result.getString("tags"), Field.Store.YES));
			
			completionService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						writer.addDocument(document);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}, null);
			
			if (count > numThreads) {
				completionService.take().get();
			}
						
			if (count % 10000 ==0 ) {
				System.out.println(count + ":" + result.getInt("id") + ":" + result.getString("title"));
			} else 	if (count % 1000 == 0 ) {
				System.out.print(".");
			}
			
		}
		
		executor.shutdown();
		System.err.println("Waiting for indexers to finish...");
		while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
			System.err.println("Still waiting for indexers to finish...");
		}
		
		writer.close();
		result.close();
		statement.close();
		connection.close();
	}
}