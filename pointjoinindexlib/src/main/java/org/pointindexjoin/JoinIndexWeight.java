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
        this.toSegments = new SingleToSegProcessor[toSearcher.getIndexReader().leaves().size()];
        for(LeafReaderContext toCtx:toSearcher.getIndexReader().leaves()) {
            toSegments[toCtx.ord]=new SingleToSegProcessor(joinIndexQuery.fromField, joinIndexQuery.toField, joinIndexQuery.indexManager, fromLeaves, toCtx);
        }
        // loop here segments and create all SegProcessors.
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
        try (SingleToSegProcessor joinConsumer = new SingleToSegProcessor(joinIndexQuery.fromField, joinIndexQuery.toField, joinIndexQuery.indexManager, fromLeaves, toContext)) { // move this precompute to createWeight()
            if (joinConsumer.isFullyIndexed()) {
                return joinConsumer.createLazy(//new SingleToSegProcessor(joinIndexQuery.fromField, joinIndexQuery.toField, joinIndexQuery.indexManager, fromLeaves, toContext).createEager(joinIndexQuery.writerFactory)
                );
            } else {
                return joinConsumer.createEager(joinIndexQuery.writerFactory);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

}
