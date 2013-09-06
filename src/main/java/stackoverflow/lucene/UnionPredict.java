package stackoverflow.lucene;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.lang3.StringUtils;

import stackoverflow.evaluate.Evaluator;

public class UnionPredict {
	private static Charset UTF8 = Charset.forName("UTF-8");
	
	public static void main(String[] args) throws Exception {
		int numToConsider = Integer.parseInt(args[0]);
		BufferedReader in;
		if (args.length > 1) {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(args[1]), UTF8));
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, UTF8));
		}
		
		CSVParser parser = new CSVParser(in);
		String[] line;

		Set<String> tags = new HashSet<String>();
		CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(System.out, UTF8), CSVStrategy.DEFAULT_STRATEGY);
		printer.print("Id");
		printer.print("Tags");
		printer.println();
		while ((line = parser.getLine()) != null) {
			tags.clear();
			int pos = 2;
			int num = 0;
			while (pos < line.length && num < numToConsider) {
				for (String tag : Evaluator.parseTags(line[pos])) {
					tags.add(tag);
				}
				num++;
				pos += 2;
			}
			printer.print(line[0]);
			printer.print(StringUtils.join(tags, ' '));
			printer.println();
		}
		printer.flush();
	}
}
