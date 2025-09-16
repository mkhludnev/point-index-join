package org.pointindexjoin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.FixedBitSet;

class SingleToSegProcessor //implements AutoCloseable
{

    final LeafReaderContext toContext;
    final private SearcherManager indexManager;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;
    private final List<FromSegIndexData> existingJoinIndices;
    private final Collection<FromSegIndexData> absentJoinIdices;
    //private final int firstAbsentOrd;
    private final String toField;
    private final String fromField;

    static class FromSegIndexData {
        final String indexValuesName;
        final JoinIndexHelper.FromContextCache fromCxt;
        PointValues joinValues;

        public FromSegIndexData(String pointIndexName, JoinIndexHelper.FromContextCache fromLeaf) {
            this.indexValuesName = pointIndexName;
            fromCxt = fromLeaf;
        }
    }

    public SingleToSegProcessor(String fromField1, String toField1,
                                SearcherManager indexManager,
                                List<JoinIndexHelper.FromContextCache> fromLeaves1,
                                LeafReaderContext toContext,
                                List<FromSegIndexData> toProcessJoin,
                                Collection<FromSegIndexData> absentJoinFields) throws IOException {
        this.toContext = toContext;
        this.indexManager = indexManager;
        this.fromLeaves = fromLeaves1;
        this.existingJoinIndices = toProcessJoin;
        this.absentJoinIdices = absentJoinFields;
        toField = toField1;
        fromField = fromField1;
    }

    public ScorerSupplier createScorerSupplier(Supplier<IndexWriter> writerFactory) throws IOException {
        // TODO guess, which of these bit sets are not necessary
        FixedBitSet exactMatchingTo;
        exactMatchingTo = new FixedBitSet(toContext.reader().maxDoc());
        EagerJoiner exactlyMatchingSink = new EagerJoiner(exactMatchingTo);
        writeJoinIndices(writerFactory, exactlyMatchingSink);

        FixedBitSet toApprox = new FixedBitSet(toContext.reader().maxDoc());
        DefaultJoinIndexReader approxSink = new DefaultJoinIndexReader(toApprox);
        readJoinIndices(approxSink);
        //assert debugBro==null || FixedBitSet.andNotCount(debugBro.toBits, toApprox)==0;
        boolean hasExactHits = exactlyMatchingSink.getAsInt() > 0;
        if (approxSink.getAsInt()>0) {
            if (hasExactHits) {
                //return new RefineTwoPhaseSupplier(toApprox,approxSink.getAsInt(),exactMatchingTo, existingJoinIndices);
                return new RefiningCertainMatchesSupplier(toApprox, approxSink.getAsInt(), existingJoinIndices,
                        exactMatchingTo);// accept exacts
            } else { // only lazy
                //return new RefineTwoPhaseSupplier(toApprox,approxSink.getAsInt(), existingJoinIndices);
                return new RefiningApproxTwoPhaseSupplier(toApprox, approxSink.getAsInt(), existingJoinIndices); //ctys
            }
        } else {
            if (hasExactHits) {
                return new BitSetScorerSupplier(exactMatchingTo, exactlyMatchingSink.getAsInt());// cty
            } else {
                return null;
            }
        }
    }

    private void writeJoinIndices(Supplier<IndexWriter> writerFactory, EagerJoiner sink) throws IOException {
        for (FromSegIndexData task : absentJoinIdices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;
            //if (fromContextCache != null) {
            JoinIndexHelper.indexJoinSegments(
                    this.indexManager, writerFactory,
                    task.fromCxt.lrc.reader().getSortedSetDocValues(fromField),
                    toContext.reader().getSortedSetDocValues(toField),
                    task.indexValuesName,
                    sink.apply(fromContextCache));
            //}
        }
    }

    private void readJoinIndices(JoinIndexReader sink) throws IOException {
        for (FromSegIndexData task : existingJoinIndices) {
            JoinIndexHelper.FromContextCache fromContextCache = task.fromCxt;
            //if (fromContextCache!=null) { // TODO it never null
            sink.readJoinIndex(fromContextCache,
                    task.joinValues);
            //}
        }
    }

    private static class EagerJoiner implements Function<JoinIndexHelper.FromContextCache, IntBinaryOperator>,
            IntSupplier {
        private final FixedBitSet toBits;
        private int hits = 0;

        public EagerJoiner(FixedBitSet toBits) {
            this.toBits = toBits;
        }

        @Override
        public IntBinaryOperator apply(JoinIndexHelper.FromContextCache fromLeaf) {
            return (f, t) -> {
                if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                    toBits.set(t);
                    hits++;
                }
                return 0;
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
