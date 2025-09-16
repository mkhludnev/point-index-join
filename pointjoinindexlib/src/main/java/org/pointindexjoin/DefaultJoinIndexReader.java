package org.pointindexjoin;

import java.io.IOException;
import java.util.function.IntSupplier;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

class DefaultJoinIndexReader implements SingleToSegProcessor.JoinIndexReader, IntSupplier {
    private final FixedBitSet toApprox;
    private int hits = 0;
    private ApproximatingJoinIndexReader visitor;

    public DefaultJoinIndexReader(FixedBitSet toApprox) {
        this.toApprox = toApprox;
    }

    @Override
    public void readJoinIndex(JoinIndexHelper.FromContextCache fromContextCache, PointValues pointValues)
            throws IOException {
        if (visitor == null) {
            visitor = new ApproximatingJoinIndexReader();
        }
        visitor.reset(fromContextCache.bits);
        visitor.intersectPointsLazy(pointValues);
        //hits += visitor.getAsInt();//??? Integer.max(hits, visitor.getAsInt()); //
    }

    @Override
    public int getAsInt() {
        return hits;
    }

    interface LazyVisitor extends PointValues.IntersectVisitor {
        boolean needsVisitDocValues();
    }

    class ApproximatingJoinIndexReader implements LazyVisitor//,
            //  IntSupplier
    {
        private FixedBitSet fromBits;
        private int upperToIdx;
        private int lowerToIdx;

        private static void intersectPointsLazy(PointValues indexPoints, LazyVisitor visitor)
                throws IOException {
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

        public void reset(FixedBitSet bits) {
            this.fromBits = bits;
        }

        public void intersectPointsLazy(PointValues indexPoints) throws IOException {
            intersectPointsLazy(indexPoints, this);
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
            if (fromBits.nextSetBit(lowerFromIdx, upperFromIdx + 1) <= upperFromIdx) {
                this.lowerToIdx = NumericUtils.sortableBytesToInt(minPackedValue, Integer.BYTES);
                this.upperToIdx = NumericUtils.sortableBytesToInt(maxPackedValue, Integer.BYTES);
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }

        // @Override
        // public int getAsInt() {
        //     return hits;
        // }
    }
}
