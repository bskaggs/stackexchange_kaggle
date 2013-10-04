package stackoverflow.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import stackoverflow.lucene.modified.MoreLikeThis;

public class NearestNeighborsFinder {
	private final MoreLikeThis mlt;
	private final IndexSearcher searcher;
	private final String searchField;
	private final int numResults;
	private final Filter filter;
	private final static Set<String> tagFields = Collections.singleton("tags");

	public NearestNeighborsFinder(MoreLikeThis mlt, IndexSearcher searcher, String searchField, Term corpusTerm, int numResults) {
		this.mlt = mlt;
		this.searcher = searcher;
		this.searchField = searchField;
		this.numResults = numResults;
		if (corpusTerm != null) {
			this.filter = new CachingWrapperFilter(new TermsFilter(corpusTerm));
		} else {
			this.filter = null;
		}
	}
	
	public String find(String docId, String text) throws IOException {
		long start = System.currentTimeMillis();
		Query query = mlt.like(new StringReader(text), searchField);
		
		long mid = System.currentTimeMillis();
		
		ScoreDoc[] hits = searcher.search(query, filter, numResults).scoreDocs;
		
		StringBuilder result = new StringBuilder();
		result.append(docId);
		for (ScoreDoc hit : hits) {
			int topId = hit.doc;
			Document doc = searcher.doc(topId, tagFields);
			String tags = doc.getField("tags").stringValue();
			
			result.append(',');
			result.append(topId);
			result.append(",\"");
			//hit.doc
			result.append(tags);
			result.append('\"');
		}
		long end = System.currentTimeMillis();
		
		result.append(";").append(mid - start).append(",").append(end - mid).append(";").append(((BooleanQuery) query).getClauses().length);
		
		return result.toString();
	}
}
