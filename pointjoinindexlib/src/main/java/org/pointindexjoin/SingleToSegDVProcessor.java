package org.pointindexjoin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * @param toField private final int firstAbsentOrd;
 */
record SingleToSegDVProcessor(String fromField, String toField, SearcherManager indexManager, LeafReaderContext toContext,
                              List<FromSegDocValuesData> existingJoinIndices,
                              Collection<FromSegDocValuesData> absentJoinIndices) //implements AutoCloseable
{
    static class FromSegDocValuesData {
        final String indexValuesName;
        final JoinIndexHelper.FromContextCache fromCxt;
        SortedNumericDocValues toDocsByFrom;

        public FromSegDocValuesData(String pointIndexName, JoinIndexHelper.FromContextCache fromLeaf) {
            this.indexValuesName = pointIndexName;
            fromCxt = fromLeaf;
        }
    }

    public ScorerSupplier createScorerSupplier(Supplier<IndexWriter> writerFactory) throws IOException {
        SingleToSegProcessor.EagerJoiner exactlyMatchingSink =
                writeJoinIndices(writerFactory, () -> new SingleToSegProcessor.EagerJoiner(new FixedBitSet(toContext.reader().maxDoc())));
        boolean hasExactHits = exactlyMatchingSink != null && exactlyMatchingSink.getAsInt() > 0;
        //AproximatingJoinIndexReader approxSink =
        DVJoinIndexReader dvJoinIndexReader = readJoinIndices(
                () -> new DVJoinIndexReader(hasExactHits ? exactlyMatchingSink.toBits :
                        new FixedBitSet(toContext.reader().maxDoc())));
        //assert debugBro==null || FixedBitSet.andNotCount(debugBro.toBits, toApprox)==0;

        if (dvJoinIndexReader == null) {
            return null;
        } else {
            return new BitSetScorerSupplier(dvJoinIndexReader.toDocs, dvJoinIndexReader.toDocs.cardinality());
        }
        // if (approxSink != null && approxSink.getAsInt() > 0) {
        //     if (hasExactHits) {
        //         //return new RefineTwoPhaseSupplier(toApprox,approxSink.getAsInt(),exactMatchingTo, existingJoinIndices);
        //         return new RefiningCertainMatchesSupplier(approxSink.toApprox, approxSink.getAsInt(), existingJoinIndices,
        //                 exactlyMatchingSink.toBits);// accept exacts
        //     } else { // only lazy
        //         //return new RefineTwoPhaseSupplier(toApprox,approxSink.getAsInt(), existingJoinIndices);
        //         return new RefiningApproxTwoPhaseSupplier(approxSink.toApprox, approxSink.getAsInt(), existingJoinIndices);
        //     }
        // } else {
        //     if (hasExactHits) {
        //         return new BitSetScorerSupplier(exactlyMatchingSink.toBits, exactlyMatchingSink.getAsInt());// cty
        //     } else {
        //         return null;
        //     }
        // }
    }

    private <R extends SingleToSegProcessor.JoinOpFactory> R writeJoinIndices(Supplier<IndexWriter> writerFactory, Supplier<R> sinkFactory)
            throws IOException {
        R sink = null;
        for (FromSegDocValuesData task : absentJoinIndices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;
            //if (fromContextCache != null) {
            if (sink == null) {
                sink = sinkFactory.get();
            }
            JoinIndexHelper.indexDVJoinSegments(
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
            for (FromSegDocValuesData task : existingJoinIndices) {
                sink.readJoinIndex(task.fromCxt, task.toDocsByFrom);
            }
            return sink;
        } else {
            return null;
        }
    }


    interface JoinIndexReader {
        void readJoinIndex(JoinIndexHelper.FromContextCache fromContextCache, SortedNumericDocValues pointValues) throws IOException;
    }

    private class DVJoinIndexReader implements JoinIndexReader {

        private final FixedBitSet toDocs;

        public DVJoinIndexReader(FixedBitSet fixedBitSet) {
            this.toDocs = fixedBitSet;
        }

        @Override
        public void readJoinIndex(JoinIndexHelper.FromContextCache fromContextCache,
                                  SortedNumericDocValues pointValues) throws IOException {
            DocIdSetIterator fromBits = new BitSetIterator(fromContextCache.bits, 1000);
            for(int fromDoc=fromBits.nextDoc(); fromDoc!=DocIdSetIterator.NO_MORE_DOCS; fromDoc=fromBits.nextDoc()){
                if(pointValues.advanceExact(fromDoc)) {
                    int numValues = pointValues.docValueCount();
                    for (int i = 0; i < numValues; i++) {
                        int toDoc = (int) pointValues.nextValue();
                        toDocs.set(toDoc); //TODO count cardinality
                    }
                }
            }
        }
    }
}
