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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    static void findIndices(IndexSearcher pointIndexSearcher, Set<String> indexFieldNames,
                            BiConsumer<FieldInfo, PointValues> downstream) throws IOException {
        for (LeafReaderContext pointIndexLeaf : pointIndexSearcher.getIndexReader().leaves()) {
            FieldInfos fieldInfos = pointIndexLeaf.reader().getFieldInfos();
            for (FieldInfo fieldInfo : fieldInfos) {
                if (indexFieldNames.contains(fieldInfo.name)) {
                    downstream.accept(fieldInfo, (pointIndexLeaf.reader()).getPointValues(fieldInfo.name));
                }
            }
        }
    }

    static AbstractMap.SimpleEntry<Map<String, PointValues>, Set<String>> extractIndices(IndexSearcher pointIndexSearcher, Set<String> indexFieldNames) throws IOException {
        Map<String, PointValues> foundIndices = new LinkedHashMap<>();
        List<String> empty = new ArrayList<>();
        findIndices(pointIndexSearcher, indexFieldNames, (f, v) -> {
            if (indexFieldNames.contains(f.name)) {
                if (f.getPointDimensionCount() == 2) {
                    foundIndices.put(f.name, v);
                } else {
                    empty.add(f.name);
                }
            }
        });
        Set<String> absent;
        if (foundIndices.size() + empty.size() < indexFieldNames.size()) {
            absent = new LinkedHashSet<>(indexFieldNames);
            absent.removeAll(empty);
            absent.removeAll(foundIndices.keySet());
        } else {
            absent = Set.of();
        }
        return new AbstractMap.SimpleEntry<>(foundIndices, absent);
    }

    static List<SingleToSegProcessor> extractIndices(List<LeafReaderContext> fromSegments, SearcherManager pointIndexManager, List<LeafReaderContext> toSegments, String fromField, String toField, List<FromContextCache> fromLeavesCached) throws IOException {
        Stream<Map.Entry<LeafReaderContext, LeafReaderContext>> fromToLeafs = fromSegments.stream().flatMap(fromLeaf -> toSegments.stream().map(toLeaf -> new AbstractMap.SimpleEntry<>(fromLeaf, toLeaf)));
        Map<String, Map.Entry<LeafReaderContext, LeafReaderContext>> fromToLeafByPointsName = fromToLeafs
                .collect(Collectors.toMap(fromTo -> getPointIndexFieldName(getSegmentName(fromTo.getKey()), getSegmentName(fromTo.getValue())), Function.identity()));
        List<ArrayList<PointValues>> indicesByTo = toSegments.stream().map(t -> new ArrayList<PointValues>(fromSegments.size())).toList();
        List<ArrayList<String>> absentPointsByTo = toSegments.stream().map(t -> new ArrayList<String>(fromSegments.size())).toList();
        // this is absolutely horrible nasty thing. We need to make it cluelessness
        fromToLeafByPointsName.entrySet().stream().forEach(toSegByFromSegByPointIndexName -> absentPointsByTo.get(toSegByFromSegByPointIndexName.getValue().getValue().ord).set(toSegByFromSegByPointIndexName.getValue().getKey().ord, toSegByFromSegByPointIndexName.getKey()));
        IndexSearcher pointIndexSegments = pointIndexManager.acquire();
        try {
            for (LeafReaderContext pointSegment : pointIndexSegments.getIndexReader().leaves()) {
                FieldInfos fieldInfos = pointSegment.reader().getFieldInfos();
                for (FieldInfo fieldInfo : fieldInfos) {
                    if (fromToLeafByPointsName.containsKey(fieldInfo.name)) {
                        Map.Entry<LeafReaderContext, LeafReaderContext> fromTo = fromToLeafByPointsName.get(fieldInfo.name);
                        if (fieldInfo.getPointDimensionCount() == 2) {
                            PointValues pointIndex = (pointSegment.reader()).getPointValues(fieldInfo.name);
                            indicesByTo.get(fromTo.getValue().ord).set(fromTo.getKey().ord, pointIndex);
                        } // else 1D tombstone
                        // anyway, wipe since it's not absent
                        absentPointsByTo.get(fromTo.getValue().ord).set(fromTo.getKey().ord, null);
                    } else {
                        // TODO removed segments
                    }
                }
            }
            // we have indices, thombstones and nulls for absents
        } finally {
            pointIndexManager.release(pointIndexSegments);
            pointIndexSegments = null;
        }
        List<SingleToSegProcessor> toSegProcessors = new ArrayList<>(indicesByTo.size());
        for (int toSegOrd = 0; toSegOrd < indicesByTo.size(); toSegOrd++) {
            toSegProcessors.set(toSegOrd, new SingleToSegProcessor(fromField, toField, pointIndexManager, fromLeavesCached, toSegments.get(toSegOrd),
                    indicesByTo.get(toSegOrd).toArray(new PointValues[]{}),
                    absentPointsByTo.get(toSegOrd).toArray(new String[]{})));
        }
//        List<SingleToSegProcessor> toSegProcessors = indicesByTo.stream().map(pointsByFrom-> new SingleToSegProcessor(pointsByFrom)).toList();
        return toSegProcessors;
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
