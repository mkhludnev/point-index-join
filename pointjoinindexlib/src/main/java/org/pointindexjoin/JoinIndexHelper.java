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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.BitSetIterator;
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

    //TO-DO reuse from and to sides of join ops <- it doesn't seem ever possible, there's nothing to cache really
    static void indexDVJoinSegments(SearcherManager indexManager, Supplier<IndexWriter> writerFactory, SortedSetDocValues fromDV
            , SortedSetDocValues toDV,
                                  String indexFieldName,
                                  IntBinOp alongSideJoin) throws IOException {

        int[] fromOrdByToOrd = innerJoinTerms(fromDV, toDV);

        Map<Integer, List<Integer>> toDocsByFromOrd = hashDV(fromOrdByToOrd, toDV);

        IntBinOp indexFromToTuple = (f, t) -> {
                alongSideJoin.applyAsInt(f, t);
        };
        //loopFrom(fromDV, toDocsByFromOrd, indexFromToTuple);
        int fromDoc,docid=0;
        IndexWriter indexWriter = null;
        while ((fromDoc = fromDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            Document doc = new Document();
            long vals = fromDV.docValueCount();
            for (int v = 0; v < vals; v++) {
                int fromOrd = (int) fromDV.nextOrd();
                List<Integer> toDocs = toDocsByFromOrd.get(fromOrd);
                if (toDocs != null) {
                    for (Integer toDoc : toDocs) {
                        doc.add(new SortedNumericDocValuesField(indexFieldName, toDoc));
                        indexFromToTuple.applyAsInt(fromDoc, toDoc);
                    }
                }
            }
            //if (doc.iterator().hasNext()) {
            for (;docid<fromDoc;) { //TODO note. we don't write such stub placeholders to the end of the index!
                Document stub = new Document();
                stub.add(new NumericDocValuesField("docid", docid++));//TODO redundant control field for assertion
                indexWriter = indexWriter ==null ? writerFactory.get():indexWriter;
                indexWriter.addDocument(stub);
            }
            doc.add(new NumericDocValuesField("docid", docid++));
            indexWriter = indexWriter == null ? writerFactory.get() : indexWriter;
            indexWriter.addDocument(doc);
            //}
        }

        if (indexWriter==null) {
             // empty tombstone
            Document tombstone = new Document();
            tombstone.add(new SortedDocValuesField(indexFieldName, new BytesRef("placeholder")));
            indexWriter = indexWriter == null ? writerFactory.get() : indexWriter;
            indexWriter.addDocument(tombstone);
        }
        indexWriter.commit();
        indexWriter.close();
        indexManager.maybeRefreshBlocking();
        Logger.getLogger(JoinIndexQuery2.class.getName()).info(() -> "written:" + indexFieldName);
    }

    static void intersectPointsLazy(PointValues indexPoints, LazyVisitor visitor)
            throws IOException {
        final PointValues.PointTree pointTree = indexPoints.getPointTree();
        while (true) {
            PointValues.Relation compare =
                    visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
            if (compare == PointValues.Relation.CELL_INSIDE_QUERY) {
                // This cell is fully inside the query shape: recursively add all points in this cell
                // without filtering
                pointTree.visitDocIDs(visitor);
            } else if (compare == PointValues.Relation.CELL_CROSSES_QUERY) {
                // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                // through and do full filtering:
                if (pointTree.moveToChild()) {
                    continue;
                }
                // TODO: we can assert that the first value here in fact matches what the pointTree
                // claimed?
                // Leaf node; scan and filter all points in this block:
                if (visitor.needsVisitDocValues()) { // TODO custom code
                    pointTree.visitDocValues(visitor);
                }
            }
            while (!pointTree.moveToSibling()) {
                if (!pointTree.moveToParent()) {
                    return;
                }
            }
        }
        //assert pointTree.moveToParent() == false;
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

    interface LazyVisitor extends PointValues.IntersectVisitor {
        default void intersectPointsLazy(PointValues indexPoints) throws IOException {
            JoinIndexHelper.intersectPointsLazy(indexPoints, this);
        }

        boolean needsVisitDocValues();
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
        final MultiRangeQuery.Relatable approxFrom;


        public FromContextCache(LeafReaderContext fromLeaf, FixedBitSet fromBits, MultiRangeQuery.Relatable approx) {
            this.lrc = fromLeaf;
            this.bits = fromBits;
            this.lowerDocId = fromBits.nextSetBit(0);
            this.upperDocId = fromBits.prevSetBit(fromBits.length() - 1);
            assert this.lowerDocId <= this.upperDocId;
            this.approxFrom = approx;
            //assert TODOcomply(fromBits, approx);
        }

        private boolean TODOcomply(FixedBitSet fromBits, MultiRangeQuery.Relatable approx) {
            BitSetIterator it = new BitSetIterator(fromBits, 0);
            boolean allSet = true;
            int doc;
            while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (!approx.matches(IntPoint.pack(doc).bytes)) {
                    allSet = false;
                    break;
                }
            }
            return allSet;
        }
    }
}
