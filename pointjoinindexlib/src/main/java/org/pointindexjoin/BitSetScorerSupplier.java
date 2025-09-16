package org.pointindexjoin;

import java.io.IOException;

import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

class BitSetScorerSupplier extends ScorerSupplier {

    private final int cardinality;
    private final FixedBitSet toBits;

    public BitSetScorerSupplier(FixedBitSet toBits, int cty) {
        this.toBits = toBits;
        cardinality = cty;
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
