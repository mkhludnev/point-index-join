package org.pointindexjoin;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;

class SingleToSegProcessor implements AutoCloseable {

    final LeafReaderContext toContext;
    final Map<String, JoinIndexHelper.FromContextCache> indexPointsNames;
    final private SearcherManager indexManager;
    final private Map<String, PointValues> pointIndices;
    final private Set<String> absent;
    private IndexSearcher pointIndexSearcher;
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
        pointIndexSearcher = null;
    }

    public ScorerSupplier createEager(Supplier<IndexWriter> writerFactory) throws IOException {
        FixedBitSet toBits = new FixedBitSet(toContext.reader().maxDoc());

        EagerJoiner sink = new EagerJoiner(toBits);

        walkAllFromSegIncSegs(writerFactory, sink);

        if (sink.hasHits()) {
            return new BitSetScorerSupplier(toBits);
        } else {
            return null;
        }
    }

    private void walkAllFromSegIncSegs(Supplier<IndexWriter> writerFactory, PointIndexConsumer sink) throws IOException {
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
    }

    interface LazyVisitor extends PointValues.IntersectVisitor{
        boolean needsVisitDocValues();
    }

    public ScorerSupplier createLazy(//SingleToSegSupplier debugBro
    ) throws IOException {
        FixedBitSet toApprox = new FixedBitSet(toContext.reader().maxDoc());
        PointIndexConsumer sink = new ApproxIndexConsumer(toApprox);

        walkAllFromSegIncSegs(null, sink);
        //assert debugBro==null || FixedBitSet.andNotCount(debugBro.toBits, toApprox)==0;
        if (sink.hasHits()) {
            return new RefineTwoPhaseSupplier(toApprox);
        } else {
            return null;
        }
    }

    private int refine(FixedBitSet toApprox, int toDocID) throws IOException {
        RefineToApproxVisitor refiner = new RefineToApproxVisitor(//toApprox,
                toDocID);
        // TODO this is a little bit awkward, it reads PointValues from closed searcher, how could it's possible?
        for (Map.Entry<String, PointValues> joinIndexByName : pointIndices.entrySet()) {
            refiner.fromCtxLeaf = indexPointsNames.get(joinIndexByName.getKey());
            joinIndexByName.getValue().intersect(refiner);
        }
        //assert refiner.minUpperSeen < Integer.MAX_VALUE;
        int lenAvailable = toDocID+refiner.eagerFetch<toApprox.length() ?
                refiner.eagerFetch : toApprox.length()-toDocID;
        FixedBitSet.andRange(refiner.toRefined,0,toApprox, toDocID, lenAvailable);
        return toDocID+ lenAvailable;
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
                if(visitor.needsVisitDocValues()) { // TODO custom code
                    pointTree.visitDocValues(visitor);
                }
            }
            while (pointTree.moveToSibling() == false) {
                if (pointTree.moveToParent() == false) {
                    return;
                }
            }
        }
        //assert pointTree.moveToParent() == false;
    }

    private static class ApproxDumper implements LazyVisitor, BooleanSupplier {
        private final FixedBitSet toApprox;
        private FixedBitSet fromBits;
        private int upperToIdx;
        private int lowerToIdx;
        private boolean hasHits;

        public ApproxDumper(FixedBitSet fromCtx, FixedBitSet toApprox) {
            this.toApprox = toApprox;
            fromBits = fromCtx;
        }

        @Override
        public boolean needsVisitDocValues() {
            toApprox.set(lowerToIdx, upperToIdx+1);
            hasHits=true;
            return false; // this trick gives all-bits approximation due to using
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
            if(fromBits.nextSetBit(lowerFromIdx,upperFromIdx+1)<=upperFromIdx) {
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

    private static class RefineToApproxVisitor implements PointValues.IntersectVisitor {
        private JoinIndexHelper.FromContextCache fromCtxLeaf;
        //private final FixedBitSet toApprox;
        private final int toDocID;
        //int minUpperSeen;
        //private int theLastUpperToIdx;
        final int eagerFetch = Long.BYTES*8;
        private FixedBitSet toRefined = new FixedBitSet(eagerFetch);

        public RefineToApproxVisitor(//FixedBitSet toApprox,
                                     int toDocID) {
            //this.toApprox = toApprox;
            this.toDocID = toDocID;
            //minUpperSeen = Integer.MAX_VALUE;
        }

        @Override
        public void visit(int docID) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            int fromIdx = NumericUtils.sortableBytesToInt(packedValue, 0);
            int toIdx = NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES);
            //minUpperSeen = Math.min(minUpperSeen, theLastUpperToIdx); // sadly it's repeated many times per leaf

            if (toIdx>=this.toDocID && toIdx<this.toDocID+eagerFetch) {
                if (fromCtxLeaf.bits.get(fromIdx)) {
                    int refineBitShifted = toIdx - this.toDocID;
                    toRefined.set(refineBitShifted); //no need to ever set it since we refine
                }
            }
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            int lowerFromIdx = NumericUtils.sortableBytesToInt(minPackedValue, 0);
            int upperFromIdx = NumericUtils.sortableBytesToInt(maxPackedValue, 0);

            int lowerToIdx = NumericUtils.sortableBytesToInt(minPackedValue, Integer.BYTES);
            int upperToIdx = NumericUtils.sortableBytesToInt(maxPackedValue, Integer.BYTES);

            if (fromCtxLeaf.upperDocId < lowerFromIdx || upperFromIdx < fromCtxLeaf.lowerDocId ||
                    toDocID + eagerFetch < lowerToIdx || upperToIdx < toDocID) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            } /*else if (lowerFromQ >= lowerFromIdx && upperFromIdx <= upperFromQdocNum) {
        return PointValues.Relation.CELL_CROSSES_QUERY;//CELL_INSIDE_QUERY;  - otherwise it misses the pointstheLastUpperToIdx

    }*/
            //theLastUpperToIdx = upperToIdx;
            return PointValues.Relation.CELL_CROSSES_QUERY;
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

    private static class EagerJoiner implements PointIndexConsumer {
        private final FixedBitSet toBits;
        private boolean hasHits = false;

        public EagerJoiner(FixedBitSet toBits) {
            this.toBits = toBits;
        }

        @Override
        public void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues indexPoints) throws IOException {
            // TODO track emptiness
            JoinIndexHelper.InnerJoinVisitor toBitsDumper = new JoinIndexHelper.InnerJoinVisitor(fromCtx.bits, toBits,
                    fromCtx.lowerDocId, fromCtx.upperDocId);
            indexPoints.intersect(toBitsDumper);
            hasHits|=toBitsDumper.getAsBoolean();
        }

        @Override
        public IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromLeaf) {
            // TODO return void
            return (f, t) -> {
                if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                    toBits.set(t);
                    hasHits=true;
                }
                return 0;
            };
        }

        @Override
        public boolean hasHits() {
            return hasHits;
        }
    }

    private static class ApproxIndexConsumer implements PointIndexConsumer {
        private final FixedBitSet toApprox;
        private boolean hasHits = false;

        public ApproxIndexConsumer(FixedBitSet toApprox) {
            this.toApprox = toApprox;
        }

        @Override
        public void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues indexPoints) throws IOException {
            // TODO track emptiness
            //PointValues.intersect((PointValues.IntersectVisitor) new JoinIndexHelper.InnerJoinVisitor(fromCtx.bits, toBits,
            //        fromCtx.lowerDocId, fromCtx.upperDocId), pointTree);
            ApproxDumper visitor = new ApproxDumper(fromCtx.bits, toApprox);
            intersectPointsLazy(indexPoints, visitor);
            hasHits|=visitor.getAsBoolean();
        }

        @Override
        public IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromLeaf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasHits() {
            return hasHits;
        }
    }

    private class RefineTwoPhaseSupplier extends ScorerSupplier {
        private final FixedBitSet toApprox;
        private final int cty;
        DocIdSetIterator approximation;

        public RefineTwoPhaseSupplier(FixedBitSet toApprox) {
            this.toApprox = toApprox;
            this.cty = toApprox.cardinality();
            approximation = new BitSetIterator(toApprox, cty);
        }

        @Override
        public Scorer get(long leadCost) throws IOException {
            //Scorer debugScorer = debugBro.get(leadCost);
            return new Scorer() {
                int refinedUpTo = -1;//exclusive

                @Override
                public TwoPhaseIterator twoPhaseIterator() {

                    return new TwoPhaseIterator(approximation) {
                        //DocIdSetIterator debugDisi = debugScorer.iterator();
                        @Override
                        public boolean matches() throws IOException {
                            int docID = approximation().docID();
                            //int debugDoc = debugDisi.advance(docID);
                            if (docID>=refinedUpTo) {
                                assert toApprox.get(docID);
                                refinedUpTo = refine(toApprox, docID);
                                assert refinedUpTo != Integer.MAX_VALUE;
                            }
                            assert docID<=refinedUpTo;
                            //assert debugBro.toBits.get(docID)==toApprox.get(docID): "refined["+docID+"]=="+toApprox.get(docID)+" exact=="+debugBro.toBits.get(docID);
                            return toApprox.get(docID);
                        }

                        @Override
                        public float matchCost() {
                            return Integer.MAX_VALUE;
                        }
                    };
                }

                @Override
                public int docID() {
                    return approximation.docID();
                }

                @Override
                public DocIdSetIterator iterator() {
                    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
                }

                @Override
                public float getMaxScore(int upTo) throws IOException {
                    return 0;
                }

                @Override
                public float score() throws IOException {
                    return 0;
                }
            };
        }

        @Override
        public long cost() {
            return cty;
        }
    }
}
