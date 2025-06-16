package org.pointindexjoin;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;

class JoinIndexWeight extends Weight {

    private final JoinIndexQuery joinIndexQuery;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;
    private final SingleToSegProcessor[] toSegments;

    public JoinIndexWeight(IndexSearcher toSearcher, JoinIndexQuery joinIndexQuery, ScoreMode scoreMode) throws IOException {
        super(joinIndexQuery);
        this.joinIndexQuery = joinIndexQuery;
        // this.scoreMode = scoreMode;
        this.fromLeaves = joinIndexQuery.cacheFromQuery(); // TODO defer it even further
        this.toSegments = JoinIndexHelper.extractIndices(joinIndexQuery.fromSearcher.getIndexReader().leaves(),
                joinIndexQuery.indexManager,
                toSearcher.getIndexReader().leaves(),
                joinIndexQuery.fromField,
                joinIndexQuery.toField,
                fromLeaves
        );
        //this.toSegments = toSegProcss.toArray(new SingleToSegProcessor[]{});
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return null;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext toContext) throws IOException {
        if (fromLeaves.isEmpty()) {
            return null;
        }
        SingleToSegProcessor joinConsumer = toSegments[toContext.ord];
        if (joinConsumer.isFullyIndexed()) {
            return joinConsumer.createLazy(//new SingleToSegProcessor(joinIndexQuery.fromField, joinIndexQuery.toField, joinIndexQuery.indexManager, fromLeaves, toContext).createEager(joinIndexQuery.writerFactory)
            );
        } else {
            return joinConsumer.createEager(joinIndexQuery.writerFactory);
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

}
