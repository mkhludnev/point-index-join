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
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;

class SingleToSegProcessor //implements AutoCloseable
{

    final LeafReaderContext toContext;
    //    final Map<String, JoinIndexHelper.FromContextCache> indexPointsNames;
    final private SearcherManager indexManager;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;
    private final PointValues[] pointValuesByFromSegOrd;
    private final String[] absentIndexNamesByFromOrd;
    private final int firstAbsentOrd;
    private final String toField;
    private final String fromField;
    //    final private Map<String, PointValues> pointIndices;
//    final private Set<String> absent;
    private IndexSearcher pointIndexSearcher;

    public SingleToSegProcessor(String fromField1, String toField1,
                                SearcherManager indexManager,
                                List<JoinIndexHelper.FromContextCache> fromLeaves1,
                                LeafReaderContext toContext,
                                PointValues[] pointValuesByFromSegOrd,
                                String[] absentIndexNamesByFromOrd) throws IOException {
        this.toContext = toContext;
        this.indexManager = indexManager;
        this.fromLeaves = fromLeaves1;
        this.pointValuesByFromSegOrd = pointValuesByFromSegOrd;
        this.absentIndexNamesByFromOrd = absentIndexNamesByFromOrd;
        this.firstAbsentOrd = indexOfNonNullOrNeg(absentIndexNamesByFromOrd);
        toField = toField1;
        fromField = fromField1;
    }

    private static int indexOfNonNullOrNeg(String[] absentIndexNamesByFromOrd) {
        for (int i = 0; i < absentIndexNamesByFromOrd.length; i++) {
            if (absentIndexNamesByFromOrd[i] != null) {
                return i;
            }
        }
        return -1;
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

//    @Override
//    public void close() throws Exception {
//        this.indexManager.release(pointIndexSearcher);
//        pointIndexSearcher = null;
//    }

    /*public boolean isFullyIndexed() {
        return firstAbsentOrd < 0;
    }

    public ScorerSupplier createEager(Supplier<IndexWriter> writerFactory) throws IOException {
        FixedBitSet toBits = new FixedBitSet(toContext.reader().maxDoc());

        EagerJoiner sink = new EagerJoiner(toBits);

        walkAllFromSegIncSegs(writerFactory, sink, true, true);

        if (sink.hasHits()) {
            return new BitSetScorerSupplier(toBits);
        } else {
            return null;
        }
    }
*/
    public ScorerSupplier createExactBitsFromAbsentSegs(Supplier<IndexWriter> writerFactory) throws IOException {
        // todo only absents
        FixedBitSet matchingTo = new FixedBitSet(toContext.reader().maxDoc());
        EagerJoiner exactlyMatchingSink = new EagerJoiner(matchingTo) {
            @Override
            public void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues indexPoints) throws IOException {
                throw new UnsupportedOperationException();
            }
        };
        walkAllFromSegIncSegs(writerFactory, exactlyMatchingSink, false, true);

        FixedBitSet toApprox = new FixedBitSet(toContext.reader().maxDoc());
        PointIndexConsumer approxSink = new ApproxIndexConsumer(toApprox);

        walkAllFromSegIncSegs(null, approxSink, true, false);
        //assert debugBro==null || FixedBitSet.andNotCount(debugBro.toBits, toApprox)==0;
        if (approxSink.hasHits()) {
            if (exactlyMatchingSink.hasHits()) {
                return new RefineTwoPhaseSupplier(toApprox, matchingTo);// accept exacts
            } else { // only lazy
                return new RefineTwoPhaseSupplier(toApprox);
            }
        } else {
            if (exactlyMatchingSink.hasHits()) {
                return new BitSetScorerSupplier(matchingTo);
            } else {
                return null;
            }
        }
    }

    private void walkAllFromSegIncSegs(Supplier<IndexWriter> writerFactory, PointIndexConsumer sink, boolean walkExisting, boolean walkAbsentAndIndex) throws IOException {

        if(walkExisting) {
            for (JoinIndexHelper.FromContextCache fromLeaf : fromLeaves) {
                if (fromLeaf != null && pointValuesByFromSegOrd[fromLeaf.lrc.ord] != null) {
                    sink.onIndexPage(fromLeaf, pointValuesByFromSegOrd[fromLeaf.lrc.ord]);
                }
            }
        }
        if (walkAbsentAndIndex && firstAbsentOrd >= 0) {
            for (JoinIndexHelper.FromContextCache fromLeaf : //firstAbsentOrd >= 0 ?
                    fromLeaves
                //        .subList(firstAbsentOrd, fromLeaves.size()) : List.<JoinIndexHelper.FromContextCache>of()
            ) {
                if (fromLeaf.lrc.ord < firstAbsentOrd) {//TODO skip by []
                    continue;
                }
                if (absentIndexNamesByFromOrd[fromLeaf.lrc.ord] != null) {
                    JoinIndexHelper.indexJoinSegments(
                            this.indexManager, writerFactory,
                            fromLeaf.lrc.reader().getSortedSetDocValues(fromField),
                            toContext.reader().getSortedSetDocValues(toField),
                            absentIndexNamesByFromOrd[fromLeaf.lrc.ord],
                            sink.createTupleConsumer(fromLeaf));
                }
            }
        }
    }

    /*public ScorerSupplier createLazy(//SingleToSegSupplier debugBro
    ) throws IOException {
        FixedBitSet toApprox = new FixedBitSet(toContext.reader().maxDoc());
        PointIndexConsumer sink = new ApproxIndexConsumer(toApprox);

        walkAllFromSegIncSegs(null, sink, true, false);
        //assert debugBro==null || FixedBitSet.andNotCount(debugBro.toBits, toApprox)==0;
        if (sink.hasHits()) {
            return new RefineTwoPhaseSupplier(toApprox);
        } else {
            return null;
        }
    }*/

    private int refine(FixedBitSet toApprox, int startingAtToDoc, FixedBitSet matchingForSure) throws IOException {
        RefineToApproxVisitor refiner = new RefineToApproxVisitor(//toApprox,
                startingAtToDoc);
        for (JoinIndexHelper.FromContextCache cacheFrom : fromLeaves) {
            refiner.fromCtxLeaf = cacheFrom;
            if(this.pointValuesByFromSegOrd[cacheFrom.lrc.ord]!=null) {
                this.pointValuesByFromSegOrd[cacheFrom.lrc.ord].intersect(refiner);
            }//else this from leaf has hits, but they were handled by just written idx-seg
        }
        //assert refiner.minUpperSeen < Integer.MAX_VALUE;
        if (matchingForSure!=null) {
            FixedBitSet.orRange(matchingForSure,startingAtToDoc, refiner.toRefined, 0,
                    startingAtToDoc + refiner.eagerFetch < matchingForSure.length()?
            refiner.eagerFetch : matchingForSure.length() - startingAtToDoc);
        }
        int lenAvailable = startingAtToDoc + refiner.eagerFetch < toApprox.length() ?
                refiner.eagerFetch : toApprox.length() - startingAtToDoc;
        FixedBitSet.andRange(refiner.toRefined, 0, toApprox, startingAtToDoc, lenAvailable);
        return startingAtToDoc + lenAvailable;
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

    private static class RefineToApproxVisitor implements PointValues.IntersectVisitor {
        //int minUpperSeen;
        //private int theLastUpperToIdx;
        final int eagerFetch = Long.BYTES * 8;
        //private final FixedBitSet toApprox;
        private final int toDocID;
        private final FixedBitSet toRefined = new FixedBitSet(eagerFetch);
        private JoinIndexHelper.FromContextCache fromCtxLeaf;

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

            if (toIdx >= this.toDocID && toIdx < this.toDocID + eagerFetch) {
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
            JoinIndexHelper.InnerJoinVisitor toBitsDumper = new JoinIndexHelper.InnerJoinVisitor(fromCtx.bits, toBits,
                    fromCtx.lowerDocId, fromCtx.upperDocId);
            indexPoints.intersect(toBitsDumper);
            hasHits |= toBitsDumper.getAsBoolean();
        }

        @Override
        public IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromLeaf) {
            // TODO return void
            return (f, t) -> {
                if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                    toBits.set(t);
                    hasHits = true;
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
            hasHits |= visitor.getAsBoolean();
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
        private final FixedBitSet matchingForSure;
        DocIdSetIterator approximation;

        public RefineTwoPhaseSupplier(FixedBitSet toApprox) {
            this.toApprox = toApprox;
            this.cty = toApprox.cardinality();
            approximation = new BitSetIterator(toApprox, cty);
            matchingForSure = null;
        }

        public RefineTwoPhaseSupplier(FixedBitSet toApprox, FixedBitSet matchingTo) {
            this.toApprox = toApprox;
            this.cty = toApprox.cardinality();
            this.toApprox.or(matchingTo);
            this.matchingForSure = matchingTo;
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
                            if (matchingForSure!=null) {
                                if (matchingForSure.get(docID)){
                                    return true;
                                }
                            }
                            //int debugDoc = debugDisi.advance(docID);
                            if (docID >= refinedUpTo) {
                                assert toApprox.get(docID);
                                refinedUpTo = refine(toApprox, docID, matchingForSure);
                                assert refinedUpTo != Integer.MAX_VALUE;
                            }
                            assert docID <= refinedUpTo;
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
