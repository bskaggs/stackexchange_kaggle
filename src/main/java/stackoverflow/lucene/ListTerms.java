package stackoverflow.lucene;

import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

public class ListTerms {
	public static void main(String[] args) throws IOException {
		DirectoryReader searchReader = DirectoryReader.open(new MMapDirectory(new File(args[0])));
		Fields fields = MultiFields.getFields(searchReader);
		
		final TObjectLongHashMap<String> counts = new TObjectLongHashMap<String>();
		String field = "text";
		Terms terms = fields.terms(field);
		TermsEnum termsEnum = terms.iterator(null);
		BytesRef text;
	    while((text = termsEnum.next()) != null) {
	    	counts.put(text.utf8ToString(), termsEnum.totalTermFreq());
	    }
	    
	    String[] keys = counts.keys(new String[counts.size()]);
	    Arrays.sort(keys, new Comparator<String>() {
			@Override
			public int compare(String k1, String k2) {
				long c1 = counts.get(k1);
				long c2 = counts.get(k2);
				if (c1 < c2) {
					return 1;
				} 
				if (c2 < c1) {
					return -1;
				}
				return 0;
			}
	    });
	    
	    for (String key : keys) {
	    	System.out.println(counts.get(key) + "\t" + key);
	    }
	
	}
}
