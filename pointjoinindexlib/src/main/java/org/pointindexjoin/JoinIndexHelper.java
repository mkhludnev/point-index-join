package org.pointindexjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

public class JoinIndexHelper {
    static final int EMPTY_JOIN_1D = 0;

    private JoinIndexHelper() {
    }

    static void loopFrom(SortedSetDocValues fromDV, Map<Integer, List<Integer>> toDocsByFromOrd, IntBinOp sink)
            throws IOException {
        int fromDoc;
        while ((fromDoc = fromDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            long vals = fromDV.docValueCount();
            for (int v = 0; v < vals; v++) {
                int fromOrd = (int) fromDV.nextOrd();
                List<Integer> toDocs = toDocsByFromOrd.get(fromOrd);
                if (toDocs != null) {
                    for (Integer toDoc : toDocs) {
                        sink.applyAsInt(fromDoc, toDoc);
                    }
                }
            }
        }
    }

    /**
     * TODO use parallel arrays
     * @return toDocs[fromOrd][]
     */
    static Map<Integer, List<Integer>> hashDV(int[] fromOrdByToOrd, SortedSetDocValues toDV) throws IOException {
        Map<Integer, List<Integer>> toDocByFromOrd = new HashMap<>();
        int toDoc;
        while ((toDoc = toDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            int vals = toDV.docValueCount();
            for (int v = 0; v < vals; v++) {
                long toOrd = toDV.nextOrd();
                int fromOrd = fromOrdByToOrd[(int) toOrd];
                if (fromOrd != DocIdSetIterator.NO_MORE_DOCS) {
                    toDocByFromOrd.computeIfAbsent(fromOrd, (k) -> new ArrayList<>()).add(toDoc);
                }
            }
        }
        return toDocByFromOrd;
    }

    /**
     * Ideas:
     *  - hash join
     *  - drive by smaller side, perhaps return reversed mapping if it's smaller
     * @return fromOrdByToOrd[ToOrd]
     */
    static int[] innerJoinTerms(SortedSetDocValues fromDV, SortedSetDocValues toDV) throws IOException {
        int[] fromOrdByToOrd = new int[(int) toDV.getValueCount()];
        Arrays.fill(fromOrdByToOrd, DocIdSetIterator.NO_MORE_DOCS);

        TermsEnum fromTerms = fromDV.termsEnum();
        TermsEnum toTerms = toDV.termsEnum();

        BytesRef fromTerm = fromTerms.next();
        BytesRef toTerm = toTerms.next();

        while (fromTerm != null && toTerm != null) {
            int cmp = fromTerm.compareTo(toTerm);
            if (cmp < 0) {
                fromTerm = fromTerms.next();
            } else if (cmp > 0) {
                toTerm = toTerms.next();
            } else {
                // Terms match: record mapping from toDV ordinal to fromDV ordinal
                int toOrd = (int) toTerms.ord();
                int fromOrd = (int) fromTerms.ord();
                fromOrdByToOrd[toOrd] = fromOrd;
                fromTerm = fromTerms.next();
                toTerm = toTerms.next();
            }
        }
        return fromOrdByToOrd;
    }

    static String getSegmentName(LeafReaderContext context) {
        return ((SegmentReader) context.reader()).getSegmentName();
    }

    static String getPointIndexFieldName(String fromSegmentName, String toSegmentName) {
        return fromSegmentName + "\u22ca" + toSegmentName;
    }

    //TO-DO reuse from and to sides of join ops <- it doesn't seem ever possible, there's nothing to cache really
    static void indexJoinSegments(SearcherManager indexManager, Supplier<IndexWriter> writerFactory, SortedSetDocValues fromDV, SortedSetDocValues toDV,
                                  String indexFieldName,
                                  IntBinOp alongSideJoin) throws IOException {

        int[] fromOrdByToOrd = innerJoinTerms(fromDV, toDV);

        Map<Integer, List<Integer>> toDocsByFromOrd = hashDV(fromOrdByToOrd, toDV);

        Document pointIdxDoc = new Document();
        IntBinOp indexFromToTuple = (f, t) -> {
            pointIdxDoc.add(
                    new IntPoint(indexFieldName, f, t));
            alongSideJoin.applyAsInt(f, t);
        };
        loopFrom(fromDV, toDocsByFromOrd, indexFromToTuple);
        IndexWriter indexWriter = writerFactory.get();
        if (pointIdxDoc.iterator().hasNext()) {
            indexWriter.addDocument(pointIdxDoc);
        } else { // empty tombstone
            pointIdxDoc.add(new IntPoint(indexFieldName, EMPTY_JOIN_1D));
            indexWriter.addDocument(pointIdxDoc);
        }
        indexWriter.close();
        indexManager.maybeRefreshBlocking();
        Logger.getLogger(JoinIndexQuery.class.getName()).info(() -> "written:" + indexFieldName);
    }

    @FunctionalInterface
    public interface IntBinOp {

        /**
         * Applies this operator to the given operands.
         *
         * @param left  the first operand
         * @param right the second operand
         */
        void applyAsInt(int left, int right);
    }
    /**
     * used across to segments, potentially might be reused acroos repeating "from" queries
     * So far they are has only live bits. It migth not work if there's an undelete op, which i'm nit aware of.
     * */
    static class FromContextCache {
        final LeafReaderContext lrc;
        final int lowerDocId;
        final int upperDocId;
        final FixedBitSet bits;


        public FromContextCache(LeafReaderContext fromLeaf, FixedBitSet fromBits) {
            this.lrc = fromLeaf;
            this.bits = fromBits;
            this.lowerDocId = fromBits.nextSetBit(0);
            this.upperDocId = fromBits.prevSetBit(fromBits.length() - 1);
            assert this.lowerDocId <= this.upperDocId;
        }
    }
}
