package stackoverflow.lucene;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVParser;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import stackoverflow.evaluate.Evaluator;

public class NearestNeighborToVowpalFormat {
	private static Charset UTF8 = Charset.forName("UTF-8");
	
	public static void main(String[] args) throws Exception {
		Class.forName("org.sqlite.JDBC");
		Connection connection = DriverManager.getConnection(args[0]);
		
		StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_44);
		int numToConsider = Integer.parseInt(args[1]);
		boolean train = Boolean.parseBoolean(args[2]);
		
		PreparedStatement statement;
		if (train) {
			statement = connection.prepareStatement("SELECT body, title, tags FROM questions WHERE id = ? LIMIT 1");
		} else {
			statement = connection.prepareStatement("SELECT body, title FROM questions WHERE id = ? LIMIT 1");
		}
		BufferedReader in;
		if (args.length > 3) {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(args[3]), UTF8));
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, UTF8));
		}
		
		CSVParser parser = new CSVParser(in);
		String[] line;

		Set<String> possibleTags = new HashSet<String>();
		TObjectIntHashMap<String> words = new TObjectIntHashMap<String>();
		final StringBuilder document = new StringBuilder();
		
		final Pattern decolonizer = Pattern.compile(":");
		
		while ((line = parser.getLine()) != null) {
			words.clear();
			document.setLength(0);
			possibleTags.clear();
			
			statement.setInt(1, Integer.parseInt(line[0]));
			ResultSet results = statement.executeQuery();
			Set<String> actualTags = null;
			String body = null;
			String title = null;
			
			while (results.next()) {
				if (train) {
					actualTags = Evaluator.parseTagsAsSet(results.getString(3));
				}
				body = results.getString(1);
				title = results.getString(2);
			}
			
			TokenStream stream = analyzer.tokenStream("body", body);
			stream.reset();
			while (stream.incrementToken()) {
				words.adjustOrPutValue(stream.getAttribute(CharTermAttribute.class).toString(), 1, 1);
			}
			stream.end();
			stream.close();
			
			stream = analyzer.tokenStream("title", title);
			stream.reset();
			while (stream.incrementToken()) {
				words.adjustOrPutValue(stream.getAttribute(CharTermAttribute.class).toString(), 1, 1);
			}
			stream.end();
			stream.close();
			
			
			words.forEachEntry(new TObjectIntProcedure<String>() {
				@Override
				public boolean execute(String word, int count) {
					document.append(" _");
					document.append(decolonizer.matcher(word).replaceAll("_COLON_"));
					document.append(":").append(count);
					return true;
				}
			});
			
			int pos = 2;
			int num = 0;
			while (pos < line.length && num < numToConsider) {
				for (String tag : Evaluator.parseTags(line[pos])) {
					possibleTags.add(tag);
				}
				num++;
				pos += 2;
			}
			
			for (String tag : possibleTags) {
				if (train) {
					System.out.print(actualTags.contains(tag) ? 1 : -1);
				}
				System.out.print(" ");
				System.out.print(line[0]);
				System.out.print("^");
				System.out.print(tag);
				System.out.print("|");
				System.out.print(tag);
				System.out.print(" ");
				System.out.println(document);
			}
		}
		analyzer.close();
		statement.close();
		connection.close();
	}
}
