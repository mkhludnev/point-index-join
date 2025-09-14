package org.pointindexjoin;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

class RefineTwoPhaseSupplier extends ScorerSupplier {
    private final List<SingleToSegProcessor.FromSegIndexData> existingJoinIndices;
    private final FixedBitSet toApprox;
    private final FixedBitSet matchingForSure;
    private final int approxHits;
    DocIdSetIterator approximation;

    public RefineTwoPhaseSupplier(FixedBitSet toApprox, int approxHits,
                                  List<SingleToSegProcessor.FromSegIndexData> existingJoinIndices) {
        this(toApprox, approxHits, null, existingJoinIndices);
    }

    public RefineTwoPhaseSupplier(FixedBitSet toApprox, int approxHits,
                                  FixedBitSet exactMatchingTo,
                                  List<SingleToSegProcessor.FromSegIndexData> existingJoinIndices) {
        this.toApprox = toApprox;
        approximation = new BitSetIterator(toApprox, this.approxHits = approxHits);
        this.existingJoinIndices = existingJoinIndices;
        if (exactMatchingTo != null) {
            this.toApprox.or(exactMatchingTo);
        }
        this.matchingForSure = exactMatchingTo;

    }

    private int refine(FixedBitSet toApprox, int startingAtToDoc, FixedBitSet matchingForSure) throws IOException {
        RefineToApproxVisitor refiner = new RefineToApproxVisitor(//toApprox,
                startingAtToDoc);

        for (SingleToSegProcessor.FromSegIndexData task : this.existingJoinIndices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;
            if (fromContextCache != null) {
                refiner.fromCtxLeaf = fromContextCache;
                task.joinValues.intersect(refiner);
            }
        }
        //assert refiner.minUpperSeen < Integer.MAX_VALUE;
        if (matchingForSure != null) {
            FixedBitSet.orRange(matchingForSure, startingAtToDoc, refiner.toRefined, 0,
                    startingAtToDoc + refiner.eagerFetch < matchingForSure.length() ?
                            refiner.eagerFetch : matchingForSure.length() - startingAtToDoc);
        }
        int lenAvailable = startingAtToDoc + refiner.eagerFetch < toApprox.length() ?
                refiner.eagerFetch : toApprox.length() - startingAtToDoc;
        FixedBitSet.andRange(refiner.toRefined, 0, toApprox, startingAtToDoc, lenAvailable);
        return startingAtToDoc + lenAvailable;
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
                        if (matchingForSure != null) {
                            if (matchingForSure.get(docID)) {
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
                        //assert debugBro.toBits.get(docID)==toApprox.get(docID): "refined["+docID+"]=="+toApprox.get(docID)+"
                        // exact=="+debugBro.toBits.get(docID);
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
        return approxHits;
    }

    static class RefineToApproxVisitor implements PointValues.IntersectVisitor {
        //int minUpperSeen;
        //private int theLastUpperToIdx;
        final int eagerFetch = Long.BYTES * 8;
        //private final FixedBitSet toApprox;
        private final int toDocID;
        final FixedBitSet toRefined = new FixedBitSet(eagerFetch);
        JoinIndexHelper.FromContextCache fromCtxLeaf;

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
                    toRefined.set(refineBitShifted); //no need to ever set it since we refine???
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
}
