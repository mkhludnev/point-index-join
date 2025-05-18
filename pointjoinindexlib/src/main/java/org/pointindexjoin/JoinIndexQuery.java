package org.pointindexjoin;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
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
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class JoinIndexQuery extends Query {
    private final IndexSearcher fromSearcher;
    private final Query fromQuery;
    final String fromField;
    final String toField;
    final SearcherManager indexManager;
    private final Supplier<IndexWriter> writerFactory;

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
        if (rewrittenFrom!=toRewriteFrom) {
            return new JoinIndexQuery(fromSearcher, rewrittenFrom,fromField,toField,indexManager,writerFactory) {
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
        return new JoinIndexWeight(this, scoreMode);
    }

    void indexJoinSegments(SortedSetDocValues fromDV, SortedSetDocValues toDV,
                           String indexFieldName,
                           IntBinaryOperator alongSideJoin) throws IOException {

        int[] fromOrdByToOrd = JoinIndexHelper.innerJoinTerms(fromDV, toDV);

        Map<Integer, List<Integer>> toDocsByFromOrd = JoinIndexHelper.hashDV(fromOrdByToOrd, toDV);

        Document pointIdxDoc = new Document();
        IntBinaryOperator indexFromToTuple = (f, t) -> {
            pointIdxDoc.add(
                    new IntPoint(indexFieldName, f, t));
            alongSideJoin.applyAsInt(f,t);
            return 0;//TODO void
        };
        JoinIndexHelper.loopFrom(fromDV, toDocsByFromOrd, indexFromToTuple);
        IndexWriter indexWriter = writerFactory.get();
        if (pointIdxDoc.iterator().hasNext()) {
            indexWriter.addDocument(pointIdxDoc);
        } else { // empty tombstone
            pointIdxDoc.add(new IntPoint(indexFieldName, JoinIndexHelper.EMPTY_JOIN_1D));
            indexWriter.addDocument(pointIdxDoc);
        }
        indexWriter.close();
        indexManager.maybeRefreshBlocking();
        Logger.getLogger(JoinIndexQuery.class.getName()).info(() -> "written:" + indexFieldName);
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

    List<JoinIndexHelper.FromContextCache> cacheFromQuery() throws IOException {
        Weight fromQueryWeight = fromSearcher.createWeight(fromQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<JoinIndexHelper.FromContextCache> fromContextCaches = new ArrayList<>(fromSearcher.getIndexReader().leaves().size());

        for (LeafReaderContext fromLeaf : fromSearcher.getIndexReader().leaves()) {
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
