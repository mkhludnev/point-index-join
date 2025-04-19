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
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class JoinIndexQuery extends Query {
    private final IndexSearcher fromSearcher;
    private final Query fromQuery;
    private final String fromField;
    private final String toField;
    private final SearcherManager indexManager;
    private final Supplier<IndexWriter> writerFactory;

    public JoinIndexQuery(IndexSearcher fromSearcher, Query fromQuery, String fromField, String toField, SearcherManager indexManager, Supplier<IndexWriter> writerFactory) {
        this.fromSearcher = fromSearcher;
        this.fromQuery = fromQuery;
        this.fromField = fromField;
        this.toField = toField;
        this.indexManager = indexManager;
        this.writerFactory = writerFactory;
    }

    private static void loopFrom(Map<Integer, List<Integer>> toDocsByFromOrd, SortedSetDocValues fromDV, BiConsumer<Integer, Integer> sink) throws IOException {
        int fromDoc;
        while ((fromDoc = fromDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            long vals = fromDV.getValueCount();
            for (int v = 0; v < vals; v++) {
                int fromOrd = (int) fromDV.nextOrd();
                List<Integer> toDocs = toDocsByFromOrd.get(fromOrd);
                if (toDocs != null) {
                    for (Integer toDoc : toDocs) {
                        sink.accept(fromDoc, toDoc);
                    }
                }
            }
        }
    }

    /**
     * @return toDocs[fromOrd][]
     */
    private static Map<Integer, List<Integer>> hashDV(int[] fromOrdByToOrd, SortedSetDocValues toDV) throws IOException {
        Map<Integer, List<Integer>> toDocByFromOrd = new HashMap<>();
        int toDoc;
        while ((toDoc = toDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            long vals = toDV.getValueCount();
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
    private static int[] innerJoinTerms(SortedSetDocValues fromDV, SortedSetDocValues toDV) throws IOException {
        int[] fromOrdByToOrd = new int[(int) toDV.getValueCount()];
        Arrays.fill(fromOrdByToOrd, DocIdSetIterator.NO_MORE_DOCS);
        BytesRef fromTerm = null;
        BytesRef toTerm = null;
        for (long fromOrd = 0, toOrd = 0;
             fromOrd < fromDV.getValueCount() && toOrd < toDV.getValueCount(); ) {
            if (fromOrd == 0 && toOrd == 0) {//boostrap
                fromTerm = fromDV.lookupOrd(fromOrd);
                toTerm = fromDV.lookupOrd(toOrd);
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

    private static String getSegmentName(LeafReaderContext context) {
        return ((SegmentReader) context.reader()).getSegmentName();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return null;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext toContext) throws IOException {
                String toSegmentName = getSegmentName(toContext);
                FixedBitSet toBits = new FixedBitSet(toContext.reader().maxDoc());
                IndexSearcher pointIndexSearcher = JoinIndexQuery.this.indexManager.acquire();
                try {
                    // TODO approximate via sibling pages
                    for (LeafReaderContext fromLeaf : JoinIndexQuery.this.fromSearcher.getIndexReader().leaves()) {
                        String fromSegmentName = getSegmentName(fromLeaf);
                        Weight weight = JoinIndexQuery.this.fromQuery.createWeight(JoinIndexQuery.this.fromSearcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                        FixedBitSet fromBits = new FixedBitSet(fromLeaf.reader().maxDoc());// TODO reuse me across "to" leafs
                        DocIdSetIterator iterator = weight.scorer(fromLeaf).iterator();
                        String indexFieldName = fromSegmentName + toSegmentName;
                        if (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {//TODO check
                            iterator.intoBitSet(fromLeaf.reader().maxDoc(), fromBits, 0);
                            int lowerFromQ = fromBits.nextSetBit(0);
                            int upperFromQ = fromBits.prevSetBit(fromBits.length() - 1);
                            for (LeafReaderContext pointIndexLeaf : pointIndexSearcher.getIndexReader().leaves()) {
                                FieldInfos fieldInfos = pointIndexLeaf.reader().getFieldInfos();
                                FieldInfo fieldInfo = fieldInfos.fieldInfo(indexFieldName);
                                // it's gonna be 2D int point
                                if (fieldInfo.getPointDimensionCount() > 0) {
                                    PointValues indexPoints = (pointIndexLeaf.reader()).getPointValues(indexFieldName);
                                    // absent field throws exception
                                    indexPoints.intersect(new InnerJoinVisitor(fromBits, toBits, upperFromQ, lowerFromQ));
                                }
                            }
                        } // hell, no index segment found. Write it and reopen.
                        // TODO what if it was just skipped by fromQ?
                        indexJoinSegments(toContext, fromLeaf, indexFieldName, fromBits, toBits);
                    }
                } finally {
                    JoinIndexQuery.this.indexManager.release(pointIndexSearcher);
                    pointIndexSearcher = null;
                }

                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        return new ConstantScoreScorer(1f, scoreMode, new BitSetIterator(toBits, toBits.cardinality()));
                    }

                    @Override
                    public long cost() {
                        return toBits.cardinality();
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    private void indexJoinSegments(LeafReaderContext toContext, LeafReaderContext fromLeaf, String indexFieldName, FixedBitSet fromBits, FixedBitSet toBits) throws IOException {
        SortedSetDocValues fromDV = fromLeaf.reader().getSortedSetDocValues(JoinIndexQuery.this.fromField);
        SortedSetDocValues toDV = toContext.reader().getSortedSetDocValues(JoinIndexQuery.this.toField);
        int[] fromOrdByToOrd = innerJoinTerms(fromDV, toDV);

        Map<Integer, List<Integer>> toDocsByFromOrd = hashDV(fromOrdByToOrd, toDV);

        IndexWriter indexWriter = writerFactory.get();
        Document pointIdxDoc = new Document();
        BiConsumer<Integer, Integer> indexFromToTuple = (f, t) -> {
            pointIdxDoc.add(
                    new IntPoint(indexFieldName, f, t));
        };
        BiConsumer<Integer, Integer> alongSideJoin = (f, t) -> {
            if (fromBits.get(f)) {
                toBits.set(t);
            }
        };
        loopFrom(toDocsByFromOrd, fromDV, indexFromToTuple.andThen(alongSideJoin));
        if (pointIdxDoc.iterator().hasNext()) {
            indexWriter.addDocument(pointIdxDoc);//TODO check size
            indexWriter.close();
            indexManager.maybeRefreshBlocking();
        } // empty thombstone???
    }

    @Override
    public String toString(String s) {
        return "";
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {

    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    private static class InnerJoinVisitor implements PointValues.IntersectVisitor {
        private final FixedBitSet fromBits;
        private final FixedBitSet toBits;
        private final int upperFromQdocNum;
        private final int lowerFromQ;

        public InnerJoinVisitor(FixedBitSet fromBits, FixedBitSet toBits, int upperFromQdocNum, int lowerFromQ) {
            this.fromBits = fromBits;
            this.toBits = toBits;
            this.upperFromQdocNum = upperFromQdocNum;
            this.lowerFromQ = lowerFromQ;
        }

        @Override
        public void visit(int docID) throws IOException {
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            int fromIdx = NumericUtils.sortableBytesToInt(packedValue, 0);
            int toIdx = NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES);
            if (fromBits.get(fromIdx)) {
                toBits.set(toIdx);
            }
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            int lowerFromIdx = NumericUtils.sortableBytesToInt(minPackedValue, 0);
            int upperFromIdx = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
            if (upperFromQdocNum < lowerFromIdx || upperFromIdx < lowerFromQ) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            } else if (lowerFromQ >= lowerFromIdx && upperFromIdx <= upperFromQdocNum) {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }
}
