package org.pointindexjoin;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.logging.Logger;

class JoinIndexWeight extends Weight {

    private final JoinIndexQuery joinIndexQuery;
    private final ScoreMode scoreMode;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;

    public JoinIndexWeight(JoinIndexQuery joinIndexQuery, ScoreMode scoreMode) throws IOException {
        super(joinIndexQuery);
        this.joinIndexQuery = joinIndexQuery;
        this.scoreMode = scoreMode;
        this.fromLeaves = joinIndexQuery.cacheFromQuery(); // TODO defer it even further
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return null;
    }

    interface IndexConsumer {
        void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues idx) throws IOException;
        IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromCtx);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext toContext) throws IOException {
        if (fromLeaves.isEmpty()) {
            return null;
        }
        String toSegmentName = JoinIndexHelper.getSegmentName(toContext);
        FixedBitSet toBits = new FixedBitSet(toContext.reader().maxDoc());

        IndexConsumer joinConsumer = new IndexConsumer() {
            @Override
            public void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues indexPoints) throws IOException {
                indexPoints.intersect(new JoinIndexHelper.InnerJoinVisitor(fromCtx.bits, toBits,
                        fromCtx.lowerDocId, fromCtx.upperDocId));
            }

            @Override
            public IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromLeaf) {
                return (f, t) -> {
                    if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                        toBits.set(t);
                    }
                    return 0;
                };
            }
        };
        walkFromContexts(toContext, toSegmentName, joinConsumer);
        if (toBits.scanIsEmpty())
            return null;
        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                return new ConstantScoreScorer(1f, scoreMode, new BitSetIterator(toBits, toBits.cardinality()));
            }
            @Override
            public long cost() {
                return toBits.cardinality();
            }
        };
    }

    private void walkFromContexts(LeafReaderContext toContext, String toSegmentName, IndexConsumer joinConsumer) throws IOException {
        IndexSearcher pointIndexSearcher = joinIndexQuery.indexManager.acquire();
        try {
            // TODO approximate via sibling pages
            nextFromLeaf:
            for (JoinIndexHelper.FromContextCache fromLeaf : fromLeaves) {
                String fromSegmentName = JoinIndexHelper.getSegmentName(fromLeaf.lrc);
                String indexFieldName = JoinIndexHelper.getPointIndexFieldName(fromSegmentName, toSegmentName);
                Logger.getLogger(JoinIndexQuery.class.getName()).info(() -> "looking for :" + indexFieldName);
                for (LeafReaderContext pointIndexLeaf : pointIndexSearcher.getIndexReader().leaves()) {
                    FieldInfos fieldInfos = pointIndexLeaf.reader().getFieldInfos();
                    FieldInfo fieldInfo = fieldInfos.fieldInfo(indexFieldName);
                    if (fieldInfo != null) {
                        if (fieldInfo.getPointDimensionCount() == 1) {
                            // this index segment has no intersects
                            continue nextFromLeaf;
                        }
                        // it's gonna be 2D int point
                        if (fieldInfo.getPointDimensionCount() == 2) { // we have 2d index
                            PointValues indexPoints = (pointIndexLeaf.reader()).getPointValues(indexFieldName);
                            // absent field throws exception
                            joinConsumer.onIndexPage(fromLeaf, indexPoints);
//                            indexPoints.intersect(new JoinIndexHelper.InnerJoinVisitor(fromLeaf.bits, toBits,
//                                    fromLeaf.lowerDocId, fromLeaf.upperDocId));
                            Logger.getLogger(JoinIndexQuery.class.getName()).info(() -> "found for :" + indexFieldName);
                            continue nextFromLeaf;
                        }
                    }
                } // hell, no index segment found. Write it and reopen.
//                IntBinaryOperator fromToDocNumsSink = (f, t) -> {
//                    if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
//                        toBits.set(t);
//                    }
//                    return 0;
//                };
                joinIndexQuery.indexJoinSegments(
                        fromLeaf.lrc.reader().getSortedSetDocValues(joinIndexQuery.fromField),
                        toContext.reader().getSortedSetDocValues(joinIndexQuery.toField),
                        indexFieldName,
                        joinConsumer.createTupleConsumer(fromLeaf));
            }
        } finally {
            joinIndexQuery.indexManager.release(pointIndexSearcher);
            pointIndexSearcher = null;
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}
