package org.pointindexjoin;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

class RefiningApproxTwoPhaseSupplier extends ScorerSupplier {
    private final List<SingleToSegProcessor.FromSegIndexData> existingJoinIndices;
    static final int eagerFetch = Long.BYTES * 64;
    private final int approxHits;
    final protected FixedBitSet toApprox;
    //private final LeafReaderContext toContext;
    DocIdSetIterator approximation;
    private RefineToApproxVisitor refiner;

    public RefiningApproxTwoPhaseSupplier(FixedBitSet toApprox, int approxHits,
                                          List<SingleToSegProcessor.FromSegIndexData> existingJoinIndices) {
        this.toApprox = toApprox;
        approximation = new BitSetIterator(toApprox, this.approxHits = approxHits);
        this.existingJoinIndices = existingJoinIndices;
        //this.toContext = toContext;
    }

    protected int refine(int startingAtToDoc) throws IOException {
        if (this.refiner == null) {
            this.refiner = createRefiningVisitor();
        }

        this.refiner.setStartingDocId(startingAtToDoc);
        int wipeUpTo = Math.min(startingAtToDoc + eagerFetch, toApprox.length());
        wipeDestBeforeRefining(startingAtToDoc, wipeUpTo);

        for (SingleToSegProcessor.FromSegIndexData task : this.existingJoinIndices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;
            if (fromContextCache != null) {
                PointValues pointIndex = task.joinValues;
                refiner.refineFromSegment(fromContextCache, pointIndex);
            }
        }
        return wipeUpTo;
    }

    protected void wipeDestBeforeRefining(int startingAtToDoc, int wipeUpTo) {
        toApprox.clear(startingAtToDoc, wipeUpTo);
    }

    protected RefineToApproxVisitor createRefiningVisitor() {
        return new RefineToApproxVisitor(toApprox);
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
                        if (docID >= refinedUpTo) {
                            // if(docID+eagerFetch+toContext.docBase>988) {
                            //     Logger.getLogger(RefiningApproxTwoPhaseSupplier.class.getName()).info("hop");
                            // }
                            assert toApprox.get(docID);
                            refinedUpTo = refine(docID);
                            // Logger.getLogger(RefiningApproxTwoPhaseSupplier.class.getName()).info(
                            //         "refine ["+(docID+toContext.docBase)+".."+(refinedUpTo+toContext.docBase)+"} where "
                            //                 +toApprox.get(docID)+" and " +toApprox.get(refinedUpTo-1)+" correspondingly"
                            // );
                        }
                        assert docID <= refinedUpTo;
                        //assert debugBro.toBits.get(docID)==toApprox.get(docID): "refined["+docID+"]=="+toApprox.get(docID)+"
                        // exact=="+debugBro.toBits.get(docID);
                        return toApprox.get(docID);
                    }

                    @Override
                    public float matchCost() {
                        return approxHits;
                    }
                };
            }

            @Override
            public int docID() {
                return approximation.docID();
            }

            @Override
            public DocIdSetIterator iterator() { //TODO eagerly get bitset for top level bulk scorer
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

/*    @Override
    public BulkScorer bulkScorer() throws IOException {
        return new BulkScorer() {

            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }*/

    static class RefineToApproxVisitor implements PointValues.IntersectVisitor {
        private final FixedBitSet destination;
        private int startingDocId;
        private JoinIndexHelper.FromContextCache fromCtxLeaf;

        public RefineToApproxVisitor(FixedBitSet toApprox) {
            this.destination = toApprox;
        }

        void setStartingDocId(int startingDocId) {
            this.startingDocId = startingDocId;
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

            if (toIdx >= this.startingDocId && toIdx < this.startingDocId + eagerFetch) {
                if (fromCtxLeaf.bits.get(fromIdx)) {
                    //int refineBitShifted = toIdx - this.toDocID;
                    destination.set(toIdx); //no need to ever set it since we refine???
                }
            }
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            int lowerFromIdx = NumericUtils.sortableBytesToInt(minPackedValue, 0);
            int upperFromIdx = NumericUtils.sortableBytesToInt(maxPackedValue, 0);

            int lowerToIdx = NumericUtils.sortableBytesToInt(minPackedValue, Integer.BYTES);
            int upperToIdx = NumericUtils.sortableBytesToInt(maxPackedValue, Integer.BYTES);

            if (//fromCtxLeaf.upperDocId < lowerFromIdx || upperFromIdx < fromCtxLeaf.lowerDocId ||
                    startingDocId + eagerFetch <= lowerToIdx // it's excluding
                            || upperToIdx < startingDocId) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

            PointValues.Relation fromRelation = fromCtxLeaf.approxFrom.relate(minPackedValue, maxPackedValue);
            if (fromRelation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
               //fails assert fromCtxLeaf.upperDocId < lowerFromIdx || upperFromIdx < fromCtxLeaf.lowerDocId; // TODO
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

            //theLastUpperToIdx = upperToIdx;
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        public void refineFromSegment(JoinIndexHelper.FromContextCache fromContextCache, PointValues pointIndex)
                throws IOException {
            this.fromCtxLeaf = fromContextCache;
            pointIndex.intersect(this);
        }
    }
}
