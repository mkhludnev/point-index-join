package org.pointindexjoin;

import org.apache.lucene.index.PointValues;

import java.io.IOException;
import java.util.function.IntBinaryOperator;

interface PointIndexConsumer {
    void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues idx) throws IOException;

    IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromCtx);
    boolean hasHits();
}
