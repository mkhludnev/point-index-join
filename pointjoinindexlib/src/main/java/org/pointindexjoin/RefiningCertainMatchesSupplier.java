package org.pointindexjoin;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.FixedBitSet;

public class RefiningCertainMatchesSupplier extends RefiningApproxTwoPhaseSupplier {
    private final FixedBitSet certainMatches;

    public RefiningCertainMatchesSupplier(FixedBitSet toApprox, int approxHits,
                                          List<SingleToSegProcessor.FromSegIndexData> existingJoinIndices,
                                          FixedBitSet certainMatches) {
        super(toApprox, approxHits, existingJoinIndices);
        this.certainMatches = certainMatches;
        assert toApprox.length() == certainMatches.length();
        this.toApprox.or(certainMatches);
    }

    @Override
    protected int refine(int startingAtToDoc) throws IOException {
        int refinedUpTo = super.refine(startingAtToDoc);
        FixedBitSet.andRange(this.certainMatches, startingAtToDoc, toApprox, startingAtToDoc, refinedUpTo - startingAtToDoc);
        return refinedUpTo;
    }

    @Override
    protected void wipeDestBeforeRefining(int startingAtToDoc, int wipeUpTo) {
        // shouldn't match certainMatches
    }

    @Override
    protected RefineToApproxVisitor createRefiningVisitor() {
        return new RefineToApproxVisitor(this.certainMatches);
    }
}
