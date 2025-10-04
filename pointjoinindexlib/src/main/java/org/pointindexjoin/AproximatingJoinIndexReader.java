package org.pointindexjoin;

import java.io.IOException;
import java.util.function.IntSupplier;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

class AproximatingJoinIndexReader implements SingleToSegProcessor.JoinIndexReader, IntSupplier {
    protected final FixedBitSet toApprox;
    private int hits = 0;
    private ApproximatingIntersectingVisitor visitor;

    public AproximatingJoinIndexReader(FixedBitSet toApprox) {
        this.toApprox = toApprox;
    }

    @Override
    public void readJoinIndex(JoinIndexHelper.FromContextCache fromContextCache, PointValues pointValues)
            throws IOException {
        if (visitor == null) {
            visitor = new ApproximatingIntersectingVisitor();
        }
        visitor.reset(fromContextCache.bits, fromContextCache.approxFrom);
        visitor.intersectPointsLazy(pointValues);
        //hits += visitor.getAsInt();//??? Integer.max(hits, visitor.getAsInt()); //
    }

    @Override
    public int getAsInt() {
        return hits;
    }

    class ApproximatingIntersectingVisitor implements JoinIndexHelper.LazyVisitor//,
            //  IntSupplier
    {
        private FixedBitSet fromBits;
        private int upperToIdx;
        private int lowerToIdx;
        private MultiRangeQuery.Relatable approxFrom;

        public void reset(FixedBitSet bits, MultiRangeQuery.Relatable approxFrom) {
            this.fromBits = bits;
            this.approxFrom = approxFrom;
        }

        @Override
        public boolean needsVisitDocValues() {
            toApprox.set(lowerToIdx, upperToIdx + 1);
            hits += upperToIdx - lowerToIdx + 1; // it's darn overestimate
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
            PointValues.Relation approxRelation = this.approxFrom.relate(minPackedValue, maxPackedValue);
            if (approxRelation==PointValues.Relation.CELL_OUTSIDE_QUERY) {
                assert !(fromBits.nextSetBit(lowerFromIdx, upperFromIdx + 1) <= upperFromIdx); //TODO
                return approxRelation;
            }
            assert fromBits.nextSetBit(lowerFromIdx, upperFromIdx + 1) <= upperFromIdx; //TODO
            //if (fromBits.nextSetBit(lowerFromIdx, upperFromIdx + 1) <= upperFromIdx) {
                this.lowerToIdx = NumericUtils.sortableBytesToInt(minPackedValue, Integer.BYTES);
                this.upperToIdx = NumericUtils.sortableBytesToInt(maxPackedValue, Integer.BYTES);
                return PointValues.Relation.CELL_CROSSES_QUERY; //always cross
            //}

        }

        // @Override
        // public int getAsInt() {
        //     return hits;
        // }
    }
}
