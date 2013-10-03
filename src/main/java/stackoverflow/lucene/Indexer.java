package stackoverflow.lucene;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class Indexer {
	public static <T> void main(String[] args) throws Exception {
		
		Options options = new Options();
		options.addOption("d", true, "database");
		options.addOption("i", true, "index directory");
		options.addOption("a", true, "analyzer class. Default is standardanalyzer");
		options.addOption("s", true, "SQL to run");
		options.addOption("c", true, "Corpus name");
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
		Directory indexDir = new SimpleFSDirectory(new File(cmd.getOptionValue("i")));
		String fieldSql = cmd.getOptionValue("s");
		final TextField corpusField = new TextField("corpus", cmd.getOptionValue("c"), Field.Store.NO);
		
		String analyzerClassName = cmd.getOptionValue("a","org.apache.lucene.analysis.standard.StandardAnalyzer");
		Analyzer analyzer = getAnalyzer(analyzerClassName);

		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_44, analyzer);
		final IndexWriter writer = new IndexWriter(indexDir, config);

		String query = "SELECT id, " + fieldSql + " as text, tags FROM questions;";
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(query);
		
		int count = 0;
		int numThreads = Runtime.getRuntime().availableProcessors();
		System.err.println("Using " + numThreads + " cores");
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<Object>(executor);
		
		final FieldType type = new FieldType();
		type.setIndexed(true);
		type.setTokenized(true);
		type.setStored(false);
		type.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS);
		type.freeze();
		
		while (result.next()) {
			count ++;
			final Document document = new Document();
			
			final int id = result.getInt("id");
			final String text = result.getString("text");
			final String tags = result.getString("tags");
			
			completionService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						document.add(new IntField("id", id , Field.Store.YES));
						document.add(corpusField);
						document.add(new TextField("tags", tags, Field.Store.YES));
						document.add(new Field("text", text, type));
						writer.addDocument(document);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}, null);
			
			if (count > numThreads) {
				completionService.take().get();
			}
						
			if (count % 10000 == 0 ) {
				System.out.println(count + ":" + id + ":" + text.substring(0, Math.min(text.length(), 40)));
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

	public static Analyzer getAnalyzer(String analyzerClassName)
			throws ClassNotFoundException, NoSuchMethodException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		@SuppressWarnings("unchecked")
		Class<Analyzer> analyzerClass = (Class<Analyzer>)Class.forName(analyzerClassName);
		Constructor<Analyzer> constructor;
		try {
			constructor = analyzerClass.getConstructor(Version.class);
		} catch (NoSuchMethodException e) {
			constructor = analyzerClass.getConstructor();
		}
		Analyzer analyzer = constructor.newInstance(Version.LUCENE_44);
		return analyzer;
	}
}