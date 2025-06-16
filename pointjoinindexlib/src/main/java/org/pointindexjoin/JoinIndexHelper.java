package org.pointindexjoin;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class JoinIndexHelper {
    static final int EMPTY_JOIN_1D = 0;

    private JoinIndexHelper() {
    }

    static void loopFrom(SortedSetDocValues fromDV, Map<Integer, List<Integer>> toDocsByFromOrd, IntBinaryOperator sink) throws IOException {
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
     * @return fromOrdByToOrd[ToOrd]
     */
    static int[] innerJoinTerms(SortedSetDocValues fromDV, SortedSetDocValues toDV) throws IOException {
        int[] fromOrdByToOrd = new int[(int) toDV.getValueCount()];
        Arrays.fill(fromOrdByToOrd, DocIdSetIterator.NO_MORE_DOCS);
        BytesRef fromTerm = null;
        BytesRef toTerm = null;
        // TODO move to termEnum
        for (long fromOrd = 0, toOrd = 0;
             fromOrd < fromDV.getValueCount() && toOrd < toDV.getValueCount(); ) {
            if (fromOrd == 0 && toOrd == 0) {//boostrap
                fromTerm = fromDV.lookupOrd(fromOrd);
                toTerm = toDV.lookupOrd(toOrd);
            }
            int cmp = fromTerm.compareTo(toTerm);

            if (cmp < 0) {
                fromOrd++;
                if (fromOrd < fromDV.getValueCount()) {
                    fromTerm = fromDV.lookupOrd(fromOrd);
                    continue;
                } else {
                    break;
                }
            } else if (cmp > 0) {
                toOrd++;
                if (toOrd < toDV.getValueCount()) {
                    toTerm = toDV.lookupOrd(toOrd);
                    continue;
                } else {
                    break;
                }
            } else {
                fromOrdByToOrd[(int) toOrd] = (int) fromOrd;
                fromOrd++;
                if (fromOrd < fromDV.getValueCount()) {
                    fromTerm = fromDV.lookupOrd(fromOrd);
                } else {
                    break;
                }
                toOrd++;
                if (toOrd < toDV.getValueCount()) {
                    toTerm = toDV.lookupOrd(toOrd);
                } else {
                    break;
                }
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

    static void indexJoinSegments(SearcherManager indexManager, Supplier<IndexWriter> writerFactory, SortedSetDocValues fromDV, SortedSetDocValues toDV,
                                  String indexFieldName,
                                  IntBinaryOperator alongSideJoin) throws IOException {

        int[] fromOrdByToOrd = innerJoinTerms(fromDV, toDV);

        Map<Integer, List<Integer>> toDocsByFromOrd = hashDV(fromOrdByToOrd, toDV);

        Document pointIdxDoc = new Document();
        IntBinaryOperator indexFromToTuple = (f, t) -> {
            pointIdxDoc.add(
                    new IntPoint(indexFieldName, f, t));
            alongSideJoin.applyAsInt(f, t);
            return 0;//TODO void
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

    static SingleToSegProcessor[] extractIndices(List<LeafReaderContext> fromSegments, SearcherManager pointIndexManager, List<LeafReaderContext> toSegments, String fromField, String toField, List<FromContextCache> fromLeavesCached) throws IOException {

        // 1. Build mapping from point index name to (fromLeaf, toLeaf) pair
        Map<String, Map.Entry<LeafReaderContext, LeafReaderContext>> pointIndexNameToPair = new HashMap<>();
        for (LeafReaderContext fromLeaf : fromSegments) {
            for (LeafReaderContext toLeaf : toSegments) {
                String pointIndexName = getPointIndexFieldName(getSegmentName(fromLeaf), getSegmentName(toLeaf));
                pointIndexNameToPair.put(pointIndexName, Map.entry(fromLeaf, toLeaf));
            }
        }

        // 2. Prepare result containers
        int fromSize = fromSegments.size(), toSize = toSegments.size();
        List<PointValues[]> indicesByTo = new ArrayList<>(toSize);
        List<String[]> absentPointsByTo = new ArrayList<>(toSize);
        for (int i = 0; i < toSize; i++) {
            indicesByTo.add(new PointValues[fromSize]);
            absentPointsByTo.add(new String[fromSize]);
        }

        // 3. Mark all as absent initially
        for (var entry : pointIndexNameToPair.entrySet()) {
            int fromOrd = entry.getValue().getKey().ord;
            int toOrd = entry.getValue().getValue().ord;
            absentPointsByTo.get(toOrd)[fromOrd] = entry.getKey();
        }

        // 4. Fill found indices, clear absent markers
        IndexSearcher searcher = pointIndexManager.acquire();
        try {
            for (LeafReaderContext pointSegment : searcher.getIndexReader().leaves()) {
                FieldInfos fieldInfos = pointSegment.reader().getFieldInfos();
                for (FieldInfo fieldInfo : fieldInfos) {
                    Map.Entry<LeafReaderContext, LeafReaderContext> pair = pointIndexNameToPair.get(fieldInfo.name);
                    if (pair != null) {
                        int fromOrd = pair.getKey().ord;
                        int toOrd = pair.getValue().ord;
                        if (fieldInfo.getPointDimensionCount() == 2) {
                            indicesByTo.get(toOrd)[fromOrd] = pointSegment.reader().getPointValues(fieldInfo.name);
                        }// else tombstone
                        absentPointsByTo.get(toOrd)[fromOrd] = null; // Clear absent marker
                    }//TODO else drop redundant
                }
            }
        } finally {
            pointIndexManager.release(searcher);
        }

        // 5. Build processors for each to-segment
        SingleToSegProcessor[] processors = new SingleToSegProcessor[toSize];
        for (int toOrd = 0; toOrd < toSize; toOrd++) {
            processors[toOrd] = new SingleToSegProcessor(
                    fromField, toField, pointIndexManager, fromLeavesCached, toSegments.get(toOrd),
                    indicesByTo.get(toOrd), absentPointsByTo.get(toOrd)
            );
        }
        return processors;
    }

    static class InnerJoinVisitor implements PointValues.IntersectVisitor, BooleanSupplier {
        private final FixedBitSet fromBits;
        private final FixedBitSet toBits;
        private final int upperFromQdocNum;
        private final int lowerFromQ;
        private boolean hasHits = false;

        public InnerJoinVisitor(FixedBitSet fromBits, FixedBitSet toBits, int lowerFromQ, int upperFromQdocNum) {
            this.fromBits = fromBits;
            this.toBits = toBits;
            this.upperFromQdocNum = upperFromQdocNum;
            this.lowerFromQ = lowerFromQ;
        }

        @Override
        public void visit(int docID) throws IOException {
            throw new UnsupportedOperationException("eager for points");
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            int fromIdx = NumericUtils.sortableBytesToInt(packedValue, 0);
            int toIdx = NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES);
            if (fromBits.get(fromIdx)) {
                hasHits = true;
                toBits.set(toIdx);
            }
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            int lowerFromIdx = NumericUtils.sortableBytesToInt(minPackedValue, 0);
            int upperFromIdx = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
            if (upperFromQdocNum < lowerFromIdx || upperFromIdx < lowerFromQ) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            } /*else if (lowerFromQ >= lowerFromIdx && upperFromIdx <= upperFromQdocNum) {
                return PointValues.Relation.CELL_CROSSES_QUERY;//CELL_INSIDE_QUERY;  - otherwise it misses the points
            }*/
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        @Override
        public boolean getAsBoolean() {
            return hasHits;
        }
    }

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
