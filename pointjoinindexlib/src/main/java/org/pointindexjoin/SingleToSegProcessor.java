package org.pointindexjoin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.FixedBitSet;

/**
 * @param toField private final int firstAbsentOrd;
 */
record SingleToSegProcessor(String fromField, String toField, SearcherManager indexManager, LeafReaderContext toContext,
                            List<FromSegIndexData> existingJoinIndices,
                            Collection<FromSegIndexData> absentJoinIndices) //implements AutoCloseable
{

    static class FromSegIndexData {
        final String indexValuesName;
        final JoinIndexHelper.FromContextCache fromCxt;
        PointValues joinValues;

        public FromSegIndexData(String pointIndexName, JoinIndexHelper.FromContextCache fromLeaf) {
            this.indexValuesName = pointIndexName;
            fromCxt = fromLeaf;
        }
    }

    public ScorerSupplier createScorerSupplier(Supplier<IndexWriter> writerFactory) throws IOException {
        EagerJoiner exactlyMatchingSink =
                writeJoinIndices(writerFactory, () -> new EagerJoiner(new FixedBitSet(toContext.reader().maxDoc())));

        AproximatingJoinIndexReader approxSink =
                readJoinIndices(() -> new AproximatingJoinIndexReader(new FixedBitSet(toContext.reader().maxDoc())));
        //assert debugBro==null || FixedBitSet.andNotCount(debugBro.toBits, toApprox)==0;
        boolean hasExactHits = exactlyMatchingSink != null && exactlyMatchingSink.getAsInt() > 0;
        if (approxSink != null && approxSink.getAsInt() > 0) {
            if (hasExactHits) {
                //return new RefineTwoPhaseSupplier(toApprox,approxSink.getAsInt(),exactMatchingTo, existingJoinIndices);
                return new RefiningCertainMatchesSupplier(approxSink.toApprox, approxSink.getAsInt(), existingJoinIndices,
                        exactlyMatchingSink.toBits);// accept exacts
            } else { // only lazy
                //return new RefineTwoPhaseSupplier(toApprox,approxSink.getAsInt(), existingJoinIndices);
                return new RefiningApproxTwoPhaseSupplier(approxSink.toApprox, approxSink.getAsInt(), existingJoinIndices);
            }
        } else {
            if (hasExactHits) {
                return new BitSetScorerSupplier(exactlyMatchingSink.toBits, exactlyMatchingSink.getAsInt());// cty
            } else {
                return null;
            }
        }
    }

    private <R extends JoinOpFactory> R writeJoinIndices(Supplier<IndexWriter> writerFactory, Supplier<R> sinkFactory)
            throws IOException {
        R sink = null;
        for (FromSegIndexData task : absentJoinIndices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;
            //if (fromContextCache != null) {
            if (sink == null) {
                sink = sinkFactory.get();
            }
            JoinIndexHelper.indexJoinSegments(
                    this.indexManager, writerFactory,
                    task.fromCxt.lrc.reader().getSortedSetDocValues(fromField),
                    toContext.reader().getSortedSetDocValues(toField),
                    task.indexValuesName,
                    sink.apply(fromContextCache));
            //}
        }
        return sink;
    }

    private <R extends JoinIndexReader> R readJoinIndices(Supplier<R> sinkFactory) throws IOException {
        if (!existingJoinIndices.isEmpty()) {
            R sink = sinkFactory.get();
            for (FromSegIndexData task : existingJoinIndices) {
                sink.readJoinIndex(task.fromCxt, task.joinValues);
            }
            return sink;
        } else {
            return null;
        }
    }

    interface JoinOpFactory extends Function<JoinIndexHelper.FromContextCache, JoinIndexHelper.IntBinOp> {
    }

    private static class EagerJoiner implements JoinOpFactory,
            IntSupplier {
        private final FixedBitSet toBits;
        private int hits = 0;

        public EagerJoiner(FixedBitSet toBits) {
            this.toBits = toBits;
        }

        @Override
        public JoinIndexHelper.IntBinOp apply(JoinIndexHelper.FromContextCache fromLeaf) {
            return (f, t) -> {
                if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                    toBits.set(t);
                    hits++;
                }
            };
        }

        @Override
        public int getAsInt() {
            return hits;
        }
    }

    interface JoinIndexReader {
        void readJoinIndex(JoinIndexHelper.FromContextCache fromContextCache, PointValues pointValues) throws IOException;
    }

}
