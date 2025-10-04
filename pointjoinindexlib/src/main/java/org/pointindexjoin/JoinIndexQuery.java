package org.pointindexjoin;

import static org.pointindexjoin.MultiRangeQuery.createTree;
import static org.pointindexjoin.MultiRangeQuery.getRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;

public class JoinIndexQuery extends Query implements  AutoCloseable{
    final IndexSearcher fromSearcher;
    final String fromField;
    final String toField;
    final SearcherManager indexManager;
    final Supplier<IndexWriter> writerFactory;
    private final Query fromQuery;
    protected List<AutoCloseable> closeables = new ArrayList<>();

    public JoinIndexQuery(IndexSearcher fromSearcher, Query fromQuery, String fromField, String toField, SearcherManager indexManager, Supplier<IndexWriter> writerFactory) {
        this.fromSearcher = fromSearcher;
        this.fromQuery = fromQuery;
        this.fromField = fromField;
        this.toField = toField;
        this.indexManager = indexManager;
        this.writerFactory = writerFactory;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        ConstantScoreQuery toRewriteFrom = new ConstantScoreQuery(fromQuery);
        Query rewrittenFrom = fromSearcher.rewrite(toRewriteFrom);
        MatchNoDocsQuery matchNoDocsQuery = new MatchNoDocsQuery();
        if (rewrittenFrom.equals(matchNoDocsQuery)) {
            return matchNoDocsQuery;
        }
        if (rewrittenFrom != toRewriteFrom) {
            return new JoinIndexQuery(fromSearcher, rewrittenFrom, fromField, toField, indexManager, writerFactory) {
                {
                    this.closeables = JoinIndexQuery.this.closeables;
                }
                @Override
                public Query rewrite(IndexSearcher indexSearcher) throws IOException {
                    return this;
                }
            };
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new JoinIndexWeight(this, scoreMode, JoinIndexQuery.this.closeables::add);
    }

    @Override
    public String toString(String s) {
        return "";
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {

    }

    @Override
    public void close() throws Exception {
        for (AutoCloseable clzbls:this.closeables) {
            try {
                clzbls.close();
            } catch (Exception e) {
                //TODO log.fatal
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    static final class FromTo implements Comparable<FromTo> {
        private int from;
        private int to;

        FromTo(int from, int to) {
            this.from = from;
            this.to = to;
        }

        @Override
            public int compareTo(FromTo o) {
                return (to - from) - (o.to - o.from);
            }

        public int from() {
            return from;
        }

        public int to() {
            return to;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (FromTo) obj;
            return this.from == that.from &&
                    this.to == that.to;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, to);
        }

        @Override
        public String toString() {
            return "FromTo[" +
                    "from=" + from + ", " +
                    "to=" + to + ']';
        }

        }

    // TODO non-null elems collection
    List<JoinIndexHelper.FromContextCache> cacheFromQuery() throws IOException {
        Weight fromQueryWeight = fromSearcher.createWeight(fromQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<JoinIndexHelper.FromContextCache> fromContextCaches = new ArrayList<>(fromSearcher.getIndexReader().leaves().size());

        List<LeafReaderContext> leaves = fromSearcher.getIndexReader().leaves();
        for (int i = 0; i < leaves.size(); i++) {
            LeafReaderContext fromLeaf = leaves.get(i);
            Scorer fromScorer = fromQueryWeight.scorer(fromLeaf);
            if (fromScorer != null) {
                DocIdSetIterator iterator = fromScorer.iterator();
                int doc;
                if ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    FixedBitSet fromBits = new FixedBitSet(fromLeaf.reader().maxDoc());
                    PriorityQueue<FromTo> gapsDesc = new PriorityQueue<>(
                            31-Integer.numberOfLeadingZeros(fromLeaf.reader().maxDoc()),
                            ()->new FromTo(-1,-1)) {
                        @Override
                        protected boolean lessThan(FromTo a, FromTo b) {
                            return a.compareTo(b)<0;
                        }
                    };
                    FromTo leastGap = gapsDesc.top();
                    Bits liveDocs = fromLeaf.reader().getLiveDocs();
                    boolean hasHits = false;
                    int minDoc = DocIdSetIterator.NO_MORE_DOCS, maxDoc = -1, prevDoc=-1;
                    for (; doc < fromLeaf.reader().maxDoc(); doc = iterator.nextDoc()) {
                        //while ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        if (liveDocs==null || liveDocs.get(doc)) {
                            fromBits.set(doc);
                            minDoc = minDoc == DocIdSetIterator.NO_MORE_DOCS ? doc : minDoc;
                            maxDoc = Math.max(maxDoc, doc);
                            hasHits = true;
                            if (prevDoc>=0) {
                                if(doc-prevDoc>1) {
                                    if((doc - prevDoc) > (leastGap.to - leastGap.from)) {
                                        leastGap.from = prevDoc;
                                        leastGap.to = doc;
                                    }
                                    leastGap = gapsDesc.updateTop();
                                }
                            }
                            prevDoc = doc;
                        }
                    }
                    if (!hasHits) {
                        continue;
                    }
                    List<FromTo> gapsIncFrom = new ArrayList<>(gapsDesc.size());
                    for(FromTo gap:gapsDesc){
                        if(gap.from>=0) {
                            gapsIncFrom.add(gap);
                        }
                    }
                    Collections.sort(gapsIncFrom, Comparator.comparingInt(a -> a.from));

                    List<MultiRangeQuery.RangeClause> rangeClauses = new ArrayList<>(gapsIncFrom.size()+1);

                    List<FromTo> ranges = new ArrayList<>(gapsIncFrom.size());
                    int lastFrom = minDoc;
                    for (Iterator<FromTo> gaps = gapsIncFrom.iterator();gaps.hasNext();){
                        FromTo gap = gaps.next();
                        ranges.add(new FromTo(lastFrom, gap.from));
                        rangeClauses.add(new MultiRangeQuery.RangeClause(IntPoint.pack(lastFrom).bytes,
                                IntPoint.pack(gap.from).bytes));
                        lastFrom = gap.to;
                    }
                    ranges.add(new FromTo(lastFrom,maxDoc));
                    rangeClauses.add(new MultiRangeQuery.RangeClause(IntPoint.pack(lastFrom).bytes,
                            IntPoint.pack(maxDoc).bytes));

                    MultiRangeQuery.Relatable range;
                    final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(Integer.BYTES);
                    if (rangeClauses.size() == 1) {
                        range = getRange(rangeClauses.get(0), 1, Integer.BYTES, comparator);
                    } else {
                        range = createTree(rangeClauses, 1, Integer.BYTES, comparator);
                    }

                    fromContextCaches.add(new JoinIndexHelper.FromContextCache(fromLeaf, fromBits , range));
                }
            }
        }
        return fromContextCaches;
    }

}
