package stackoverflow.evaluate;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVParser;

public class Evaluator {
	private static Charset UTF8 = Charset.forName("UTF-8");
	
	public static void main(String[] args) throws Exception {
		Class.forName("org.sqlite.JDBC");
		Connection connection = DriverManager.getConnection(args[0]);
		PreparedStatement tagStatement = connection.prepareStatement("SELECT tags FROM questions WHERE id = ? LIMIT 1");
		
		BufferedReader in;
		if (args.length > 1) {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(args[1]), UTF8));
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, UTF8));
		}
		
		CSVParser parser = new CSVParser(in);
		String[] line;
		parser.getLine(); //header skip
		
		double f1Sum = 0;
		int count = 0;
		while ((line = parser.getLine()) != null) {
			tagStatement.setInt(1, Integer.parseInt(line[0]));
			ResultSet results = tagStatement.executeQuery();
			Set<String> expected = null;
			while (results.next()) {
				expected = parseTagsAsSet(results.getString(1));
			}
			
			int truePositives = 0;
			Set<String> predicted = parseTagsAsSet(line[1]);
			
			for (String tag : expected) {
				if (predicted.contains(tag)) {
					truePositives++;
				}
			}
			
			double precision = ((double) truePositives) / (predicted.size());
			double recall = ((double) truePositives) / (expected.size());
			double f1 = precision * recall;
			f1Sum += f1;
			count++;
			
//			System.out.println("Expected:\t" + expected);
//			System.out.println("Predicted:\t" + predicted);
//			System.out.println("Precision:\t" + precision);
//			System.out.println("Recall:\t" + recall);
//			System.out.println("F1:\t" + f1);
//			System.out.println();
			
		}
		System.out.println("Mean F1: " + (f1Sum / count));
		tagStatement.close();
		connection.close();
	}
	public static final Pattern whitespace = Pattern.compile("\\s+");
	public static String[] parseTags(String tagString) {
		return whitespace.split(tagString);
	}
	
	public static Set<String> parseTagsAsSet(String tagString) {
		return new HashSet<String>(Arrays.asList(parseTags(tagString)));
	}

}
