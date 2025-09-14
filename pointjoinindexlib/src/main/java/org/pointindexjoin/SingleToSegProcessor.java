package org.pointindexjoin;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;

class SingleToSegProcessor //implements AutoCloseable
{

    final LeafReaderContext toContext;
    final private SearcherManager indexManager;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;
    private final List<FromSegIndexData> existingJoinIndices;
    private final Collection<FromSegIndexData> absentJoinIdices;
    //private final int firstAbsentOrd;
    private final String toField;
    private final String fromField;

    static class FromSegIndexData {
        final String indexValuesName;
        final JoinIndexHelper.FromContextCache fromCxt;
        PointValues joinValues;

        public FromSegIndexData(String pointIndexName, JoinIndexHelper.FromContextCache fromLeaf) {
            this.indexValuesName = pointIndexName;
            fromCxt = fromLeaf;
        }
    }

    public SingleToSegProcessor(String fromField1, String toField1,
                                SearcherManager indexManager,
                                List<JoinIndexHelper.FromContextCache> fromLeaves1,
                                LeafReaderContext toContext,
                                List<FromSegIndexData> toProcessJoin,
                                Collection<FromSegIndexData> absentJoinFields) throws IOException {
        this.toContext = toContext;
        this.indexManager = indexManager;
        this.fromLeaves = fromLeaves1;
        this.existingJoinIndices = toProcessJoin;
        this.absentJoinIdices = absentJoinFields;
        toField = toField1;
        fromField = fromField1;
    }

    private static void intersectPointsLazy(PointValues indexPoints, LazyVisitor visitor) throws IOException {
        final PointValues.PointTree pointTree = indexPoints.getPointTree();
        while (true) {
            PointValues.Relation compare =
                    visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
            if (compare == PointValues.Relation.CELL_INSIDE_QUERY) {
                // This cell is fully inside the query shape: recursively add all points in this cell
                // without filtering
                pointTree.visitDocIDs(visitor);
            } else if (compare == PointValues.Relation.CELL_CROSSES_QUERY) {
                // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                // through and do full filtering:
                if (pointTree.moveToChild()) {
                    continue;
                }
                // TODO: we can assert that the first value here in fact matches what the pointTree
                // claimed?
                // Leaf node; scan and filter all points in this block:
                if (visitor.needsVisitDocValues()) { // TODO custom code
                    pointTree.visitDocValues(visitor);
                }
            }
            while (!pointTree.moveToSibling()) {
                if (!pointTree.moveToParent()) {
                    return;
                }
            }
        }
        //assert pointTree.moveToParent() == false;
    }

    public ScorerSupplier createScorerSuplier(Supplier<IndexWriter> writerFactory) throws IOException {
        FixedBitSet exactMatchingTo = new FixedBitSet(toContext.reader().maxDoc());
        EagerJoiner exactlyMatchingSink = new EagerJoiner(exactMatchingTo);
        writeJoinIndices(writerFactory, exactlyMatchingSink);

        FixedBitSet toApprox = new FixedBitSet(toContext.reader().maxDoc());
        JoinIndexReader approxSink = new ApproxIndexConsumer(toApprox);
        readJoinIndices(approxSink);
        //assert debugBro==null || FixedBitSet.andNotCount(debugBro.toBits, toApprox)==0;
        if (approxSink.getAsBoolean()) {
            if (exactlyMatchingSink.getAsBoolean()) {
                return new RefineTwoPhaseSupplier(toApprox, exactMatchingTo, existingJoinIndices);// accept exacts
            } else { // only lazy
                return new RefineTwoPhaseSupplier(toApprox, existingJoinIndices); //ctys
            }
        } else {
            if (exactlyMatchingSink.getAsBoolean()) {
                return new BitSetScorerSupplier(exactMatchingTo);// cty
            } else {
                return null;
            }
        }
    }

    private void writeJoinIndices(Supplier<IndexWriter> writerFactory, EagerJoiner sink) throws IOException {
        for (FromSegIndexData task : absentJoinIdices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;;
            if (fromContextCache != null) {
                JoinIndexHelper.indexJoinSegments(
                        this.indexManager, writerFactory,
                        task.fromCxt.lrc.reader().getSortedSetDocValues(fromField),
                        toContext.reader().getSortedSetDocValues(toField),
                        task.indexValuesName,
                        sink.apply(fromContextCache));
            }
        }
    }

    private void readJoinIndices(JoinIndexReader sink) throws IOException {
        for (FromSegIndexData task : existingJoinIndices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;
            if (fromContextCache!=null) { // TODO it never null
                sink.readJoinIndex(fromContextCache,
                        task.joinValues);
            }
        }
    }

    interface LazyVisitor extends PointValues.IntersectVisitor {
        boolean needsVisitDocValues();
    }

    private static class ApproxDumper implements LazyVisitor, BooleanSupplier {
        private final FixedBitSet toApprox;
        private final FixedBitSet fromBits;
        private int upperToIdx;
        private int lowerToIdx;
        private boolean hasHits;

        public ApproxDumper(FixedBitSet fromCtx, FixedBitSet toApprox) {
            this.toApprox = toApprox;
            fromBits = fromCtx;
        }

        @Override
        public boolean needsVisitDocValues() {
            toApprox.set(lowerToIdx, upperToIdx + 1);
            hasHits = true;
            return false; // TODO this trick gives all-bits approximation due to using
            // min-maxes from header (non-leaf) nodes,
            // however refining kicks in quite early.
            // to get narrow approx we need an own bkd-tree.
        }

        @Override
        public void visit(int docID) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            //toApprox.set(lowerToIdx, upperToIdx+1);
            throw new UnsupportedOperationException();
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            int lowerFromIdx = NumericUtils.sortableBytesToInt(minPackedValue, 0);
            int upperFromIdx = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
            if (fromBits.nextSetBit(lowerFromIdx, upperFromIdx + 1) <= upperFromIdx) {
                this.lowerToIdx = NumericUtils.sortableBytesToInt(minPackedValue, Integer.BYTES);
                this.upperToIdx = NumericUtils.sortableBytesToInt(maxPackedValue, Integer.BYTES);
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }

        @Override
        public boolean getAsBoolean() {
            return hasHits;
        }
    }

    private static class BitSetScorerSupplier extends ScorerSupplier {

        private final int cardinality;
        private final FixedBitSet toBits;

        public BitSetScorerSupplier(FixedBitSet toBits) {
            this.toBits = toBits;
            cardinality = toBits.cardinality();
        }

        @Override
        public Scorer get(long leadCost) throws IOException {
            return new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, new BitSetIterator(toBits, cardinality));
        }

        @Override
        public long cost() {
            return cardinality;
        }
    }

    private static class EagerJoiner implements Function<JoinIndexHelper.FromContextCache ,IntBinaryOperator>,
            BooleanSupplier{
        private final FixedBitSet toBits;
        private boolean hasHits = false;

        public EagerJoiner(FixedBitSet toBits) {
            this.toBits = toBits;
        }

        @Override
        public IntBinaryOperator apply(JoinIndexHelper.FromContextCache fromLeaf) {
            return (f, t) -> {
                if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                    toBits.set(t);
                    hasHits = true;
                }
                return 0;
            };
        }

        @Override
        public boolean getAsBoolean() {
            return hasHits;
        }
    }

    interface JoinIndexReader extends BooleanSupplier{
        void readJoinIndex(JoinIndexHelper.FromContextCache fromContextCache, PointValues pointValues) throws IOException;
    }

    private static class ApproxIndexConsumer implements JoinIndexReader {
        private final FixedBitSet toApprox;
        boolean hasHits = false;

        public ApproxIndexConsumer(FixedBitSet toApprox) {
            this.toApprox = toApprox;
        }

        @Override
        public void readJoinIndex(JoinIndexHelper.FromContextCache fromContextCache, PointValues pointValues)
                throws IOException {
            // TODO track emptiness
            //PointValues.intersect((PointValues.IntersectVisitor) new JoinIndexHelper.InnerJoinVisitor(fromCtx.bits, toBits,
            //        fromCtx.lowerDocId, fromCtx.upperDocId), pointTree);
            ApproxDumper visitor = new ApproxDumper(fromContextCache.bits, toApprox);
            intersectPointsLazy(pointValues, visitor);
            hasHits |= visitor.getAsBoolean();
        }

        @Override
        public boolean getAsBoolean() {
            return hasHits;
        }
    }

}
