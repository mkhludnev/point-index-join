package org.pointindexjoin;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;

class SingleToSegProcessor implements AutoCloseable {

    final LeafReaderContext toContext;
    final Map<String, JoinIndexHelper.FromContextCache> indexPointsNames;
    final private SearcherManager indexManager;
    final private Map<String, PointValues> pointIndices;
    final private Set<String> absent;
    final private IndexSearcher pointIndexSearcher;
    private final String toField;
    private final String fromField;

    public SingleToSegProcessor(String fromField1, String toField1, SearcherManager indexManager, List<JoinIndexHelper.FromContextCache> fromLeaves1, LeafReaderContext toContext) throws IOException {
        this.toContext = toContext;
        this.indexManager = indexManager;
        pointIndexSearcher = indexManager.acquire();

        indexPointsNames = new LinkedHashMap<>(fromLeaves1.size());
        String toSegmentName = JoinIndexHelper.getSegmentName(toContext);
        // TODO approximate via sibling pages
        //nextFromLeaf:
        for (JoinIndexHelper.FromContextCache fromLeaf : fromLeaves1) {
            String fromSegmentName = JoinIndexHelper.getSegmentName(fromLeaf.lrc);
            String indexFieldName = JoinIndexHelper.getPointIndexFieldName(fromSegmentName, toSegmentName);
            indexPointsNames.put(indexFieldName, fromLeaf);
        }
        AbstractMap.SimpleEntry<Map<String, PointValues>, Set<String>> indicesAndAbsent = JoinIndexHelper.extractIndices(pointIndexSearcher, indexPointsNames.keySet());
        this.pointIndices = indicesAndAbsent.getKey();
        this.absent = indicesAndAbsent.getValue();
        toField = toField1;
        fromField = fromField1;
    }

    public boolean isFullyIndexed() {
        return absent.isEmpty();
    }

    @Override
    public void close() throws Exception {
        this.indexManager.release(pointIndexSearcher);
        //pointIndexSearcher = null;
    }

    public ScorerSupplier createEager(Supplier<IndexWriter> writerFactory) throws IOException {
        FixedBitSet toBits = new FixedBitSet(toContext.reader().maxDoc());

        PointIndexConsumer sink = new PointIndexConsumer() {
            @Override
            public void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues indexPoints) throws IOException {
                // TODO tack emptiness
                indexPoints.intersect(new JoinIndexHelper.InnerJoinVisitor(fromCtx.bits, toBits,
                        fromCtx.lowerDocId, fromCtx.upperDocId));
            }

            @Override
            public IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromLeaf) {
                // TODO tack emptiness
                return (f, t) -> {
                    if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                        toBits.set(t);
                    }
                    return 0;
                };
            }
        };

        for (Map.Entry<String, PointValues> joinIndexByName : pointIndices.entrySet()) {
            JoinIndexHelper.FromContextCache fromCtxLeaf = indexPointsNames.get(joinIndexByName.getKey());
            sink.onIndexPage(fromCtxLeaf, joinIndexByName.getValue());
        }
        for (String absentIndexName : absent) {
            JoinIndexHelper.FromContextCache fromCtxLeaf = indexPointsNames.get(absentIndexName);
            JoinIndexHelper.indexJoinSegments(
                    this.indexManager, writerFactory,
                    fromCtxLeaf.lrc.reader().getSortedSetDocValues(fromField),
                    toContext.reader().getSortedSetDocValues(toField),
                    absentIndexName,
                    sink.createTupleConsumer(fromCtxLeaf));
        }

        if (toBits.scanIsEmpty())
            return null;
        return new ScorerSupplier() {

            private final int cardinality = toBits.cardinality();

            @Override
            public Scorer get(long leadCost) throws IOException {
                return new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, new BitSetIterator(toBits, cardinality));
            }

            @Override
            public long cost() {
                return cardinality;
            }
        };
    }

    public ScorerSupplier createLazy() {
        throw new UnsupportedOperationException();
    }
}
