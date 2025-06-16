package org.pointindexjoin;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class JoinIndexQuery extends Query {
    final IndexSearcher fromSearcher;
    final String fromField;
    final String toField;
    final SearcherManager indexManager;
    final Supplier<IndexWriter> writerFactory;
    private final Query fromQuery;

    public JoinIndexQuery(IndexSearcher fromSearcher, Query fromQuery, String fromField, String toField, SearcherManager indexManager, Supplier<IndexWriter> writerFactory) {
        this.fromSearcher = fromSearcher;
        this.fromQuery = fromQuery;
        this.fromField = fromField;
        this.toField = toField;
        this.indexManager = indexManager;
        this.writerFactory = writerFactory;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        ConstantScoreQuery toRewriteFrom = new ConstantScoreQuery(fromQuery);
        Query rewrittenFrom = fromSearcher.rewrite(toRewriteFrom);
        MatchNoDocsQuery matchNoDocsQuery = new MatchNoDocsQuery();
        if (rewrittenFrom.equals(matchNoDocsQuery)) {
            return matchNoDocsQuery;
        }
        if (rewrittenFrom != toRewriteFrom) {
            return new JoinIndexQuery(fromSearcher, rewrittenFrom, fromField, toField, indexManager, writerFactory) {
                @Override
                public Query rewrite(IndexSearcher indexSearcher) throws IOException {
                    return this;
                }
            };
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new JoinIndexWeight(searcher, this, scoreMode);
    }

    @Override
    public String toString(String s) {
        return "";
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {

    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    // TODO non-null elems collection
    List<JoinIndexHelper.FromContextCache> cacheFromQuery() throws IOException {
        Weight fromQueryWeight = fromSearcher.createWeight(fromQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<JoinIndexHelper.FromContextCache> fromContextCaches = new ArrayList<>(fromSearcher.getIndexReader().leaves().size());

        List<LeafReaderContext> leaves = fromSearcher.getIndexReader().leaves();
        for (int i = 0; i < leaves.size(); i++) {
            LeafReaderContext fromLeaf = leaves.get(i);
            Scorer fromScorer = fromQueryWeight.scorer(fromLeaf);
            if (fromScorer != null) {
                DocIdSetIterator iterator = fromScorer.iterator();
                if (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    FixedBitSet fromBits = new FixedBitSet(fromLeaf.reader().maxDoc());
                    // TODO may it be already cached in anywhere?
                    iterator.intoBitSet(fromLeaf.reader().maxDoc(), fromBits, 0);
                    fromContextCaches.add(new JoinIndexHelper.FromContextCache(fromLeaf, fromBits));
                }
            }
        }
        return fromContextCaches;
    }

}
