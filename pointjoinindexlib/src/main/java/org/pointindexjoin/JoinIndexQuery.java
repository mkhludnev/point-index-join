package org.pointindexjoin;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
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

public class JoinIndexQuery extends Query {
    private final Directory pointIndexDir;
    private final IndexSearcher fromSearcher;
    private final Query fromQuery;
    private final String fromField;
    private final String toField;

    public JoinIndexQuery(Directory jonnindexdir, IndexSearcher fromSearcher, Query fromQuery, String fromField, String toField) {
        this.pointIndexDir = jonnindexdir;
        this.fromSearcher = fromSearcher;
        this.fromQuery = fromQuery;
        this.fromField = fromField;
        this.toField = toField;
        this.fromField = fromField;
        this.toField = toField;
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
                DirectoryReader reader = DirectoryReader.open(JoinIndexQuery.this.pointIndexDir);
                IndexSearcher pointIndexSearcher = new IndexSearcher(reader);
                FixedBitSet toBits = new FixedBitSet(toContext.reader().maxDoc());
                // TODO approximate via sibling pages
                for (LeafReaderContext fromLeaf : JoinIndexQuery.this.fromSearcher.getIndexReader().leaves()) {
                    String fromSegmentName = getSegmentName(fromLeaf);
                    Weight weight = JoinIndexQuery.this.createWeight(JoinIndexQuery.this.fromSearcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    FixedBitSet fromBits = new FixedBitSet(fromLeaf.reader().maxDoc());// TODO reuse me across "to" leafs
                    weight.scorer(fromLeaf).iterator().intoBitSet(fromLeaf.reader().maxDoc(), fromBits, 0);
                    int lowerFromQ = fromBits.nextSetBit(0);
                    int upperFromQ = fromBits.prevSetBit(fromBits.length() - 1);
                    String indexFieldName = fromSegmentName + toSegmentName;
                    for (LeafReaderContext pointIndexLeaf: pointIndexSearcher.getIndexReader().leaves()){
                        FieldInfos fieldInfos = pointIndexLeaf.reader().getFieldInfos();
                        FieldInfo fieldInfo = fieldInfos.fieldInfo(indexFieldName);
                        // it's gonna be 2D int point
                        if(fieldInfo.getPointDimensionCount()>0) {
                            PointValues indexPoints = (pointIndexLeaf.reader()).getPointValues(indexFieldName);
                            // absent field throws exception
                            indexPoints.intersect(new PointValues.IntersectVisitor() {
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
                                    if (upperFromQ < lowerFromIdx || upperFromIdx < lowerFromQ) {
                                        return PointValues.Relation.CELL_OUTSIDE_QUERY;
                                    } else if (lowerFromQ >= lowerFromIdx && upperFromIdx <= upperFromQ) {
                                        return PointValues.Relation.CELL_INSIDE_QUERY;
                                    }
                                    return PointValues.Relation.CELL_CROSSES_QUERY;
                                }
                            });
                        }
                    } // hell, no index segment found. Write it and reopen.
                    SortedSetDocValues fromDV = fromLeaf.reader().getSortedSetDocValues(JoinIndexQuery.this.fromField);
                    SortedSetDocValues toDV = toContext.reader().getSortedSetDocValues(JoinIndexQuery.this.toField);
                    int[] fromOrdByToOrd = innerJoinTerms(fromDV, toDV);
                    // hash join
                    // intersect terms
                    Map<Integer, List<Integer>> toDocsByFromOrd = hashDV(fromOrdByToOrd, toDV);
                    BiConsumer<Integer, Integer> indexFromToTuple = (f, t) -> {
                        new IntPoint(indexFieldName, f,t);
                    };
                    BiConsumer<Integer, Integer> alongSideJoin = (f, t) -> {
                        if(fromBits.get(f)) {
                            toBits.set(t);
                        }
                    };
                    loopFrom(toDocsByFromOrd,fromDV, indexFromToTuple.andThen(alongSideJoin));
                }
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        return new Scorer() {
                            @Override
                            public int docID() {
                                return 0;
                            }

                            @Override
                            public DocIdSetIterator iterator() {
                                return null;
                            }

                            @Override
                            public float getMaxScore(int upTo) throws IOException {
                                return 0;
                            }

                            @Override
                            public float score() throws IOException {
                                return 0;
                            }
                        };
                    }

                    @Override
                    public long cost() {
                        return 0;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    private static  void loopFrom(Map<Integer, List<Integer>> toDocsByFromOrd, SortedSetDocValues fromDV, BiConsumer<Integer, Integer> sink) throws IOException {
        int fromDoc;
        while ((fromDoc = fromDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            long vals = fromDV.getValueCount();
            for (int v = 0; v < vals; v++) {
                long fromOrd = fromDV.nextOrd();
                List<Integer> toDocs = toDocsByFromOrd.get(fromOrd);
                if (toDocs!=null) {
                    for (Integer toDoc:toDocs) {
                        sink.accept(fromDoc,toDoc);
                    }
                }
            }
        }
    }
    /**
     * @return toDocs[fromOrd][]
     * */
    private static Map<Integer, List<Integer>> hashDV(int[] fromOrdByToOrd, SortedSetDocValues toDV) throws IOException {
        Map<Integer, List<Integer>> toDocByFromOrd = new HashMap<>();
        int toDoc;
        while ((toDoc = toDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            long vals = toDV.getValueCount();
            for (int v = 0; v< vals; v++){
                long toOrd = toDV.nextOrd();
                int fromOrd = fromOrdByToOrd[(int) toOrd];
                if (fromOrd!=DocIdSetIterator.NO_MORE_DOCS) {
                    toDocByFromOrd.computeIfAbsent(fromOrd,(k)->new ArrayList<>()).add(toDoc);
                }
            }
        }
        return toDocByFromOrd;
    }

    /**
     * @return fromOrdByToOrd[ToOrd]
     */
    private int[] innerJoinTerms(SortedSetDocValues fromDV, SortedSetDocValues toDV) throws IOException {
        int[] fromOrdByToOrd =new int[(int) toDV.getValueCount()];
        Arrays.fill(fromOrdByToOrd,DocIdSetIterator.NO_MORE_DOCS);
        BytesRef fromTerm = null;
        BytesRef toTerm = null;
        for (long fromOrd=0, toOrd=0 ;
             fromOrd < fromDV.getValueCount() && toOrd < toDV.getValueCount();) {
            if (fromOrd==0&&toOrd==0) {//boostrap
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
}
