package stackoverflow.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

public class NearestNeighborsFinder {
	private final MoreLikeThis mlt;
	private final IndexSearcher searcher;
	private final String searchField;
	private final Query corpusQuery;
	private final int numResults;
	private final static Set<String> tagFields = Collections.singleton("tags");

	public NearestNeighborsFinder(MoreLikeThis mlt, IndexSearcher searcher, String searchField, Query corpusQuery, int numResults) {
		this.mlt = mlt;
		this.searcher = searcher;
		this.searchField = searchField;
		this.corpusQuery = corpusQuery;
		this.numResults = numResults;
	}
	
	public String find(String docId, String text) throws IOException {
		Query query = mlt.like(new StringReader(text), searchField);
		
		if (corpusQuery != null) {
			((BooleanQuery) query).add(corpusQuery, Occur.MUST);
		}
		
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
		return result.toString();
	}
}
