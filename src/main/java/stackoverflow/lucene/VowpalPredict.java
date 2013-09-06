package stackoverflow.lucene;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.lang3.StringUtils;

import stackoverflow.evaluate.Evaluator;

public class VowpalPredict {
	private static Charset UTF8 = Charset.forName("UTF-8");
	
	public static void main(String[] args) throws Exception {
		BufferedReader in;
		if (args.length > 0) {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(args[1]), UTF8));
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, UTF8));
		}
		
		Set<String> tags = new HashSet<String>();
		CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(System.out, UTF8), CSVStrategy.DEFAULT_STRATEGY);
		printer.print("Id");
		printer.print("Tags");
		printer.println();
		
		String line;
		String lastNum = null;
		String bestTag = null;
		double lastBest = Double.NEGATIVE_INFINITY;
		Pattern splitter = Pattern.compile("\\^|\\s");
		while ((line = in.readLine()) != null) {
			String[] parts = splitter.split(line);
			if (lastNum != null && !lastNum.equals(parts[1])) {
				if (tags.isEmpty()) {
					tags.add(bestTag);
				}
				printer.print(lastNum);
				printer.print(StringUtils.join(tags, ' '));
				printer.println();
				tags.clear();
				lastBest = Double.NEGATIVE_INFINITY;
				bestTag = null;
			}
			double value = Double.parseDouble(parts[0]);
			String tag = parts[2];
			if (value > 0) {
				tags.add(tag);
			}
			if (value > lastBest) {
				bestTag = tag;
				lastBest = value;
			}
			lastNum = parts[1];
		}
		printer.print(lastNum);
		printer.print(StringUtils.join(tags, ' '));
		printer.println();
		printer.flush();
	}
}
