package org.pointindexjoin;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;

class JoinIndexWeight extends Weight {

    private final JoinIndexQuery joinIndexQuery;
    private final ScoreMode scoreMode;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;

    public JoinIndexWeight(JoinIndexQuery joinIndexQuery, ScoreMode scoreMode) throws IOException {
        super(joinIndexQuery);
        this.joinIndexQuery = joinIndexQuery;
        this.scoreMode = scoreMode;
        this.fromLeaves = joinIndexQuery.cacheFromQuery(); // TODO defer it even further
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

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return null;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext toContext) throws IOException {
        if (fromLeaves.isEmpty()) {
            return null;
        }
        try(EagerIndexConsumer joinConsumer = new EagerIndexConsumer(toContext)) {
            joinConsumer.walkFromContexts(toContext);
            if (!joinConsumer.indicesAndAbsent.getValue().isEmpty()) {
                joinConsumer.prepareEager();
                return joinConsumer.createEager();
            } else {
                joinConsumer.prepareLazy();
                return joinConsumer.createLazy();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    interface IndexConsumer {
        void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues idx) throws IOException;

        IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromCtx);
    }

    abstract class AbstractIndexConsumer implements IndexConsumer , AutoCloseable{

        private final LeafReaderContext toContext;
        private Map<String, JoinIndexHelper.FromContextCache> indexPointsNames;
        AbstractMap.SimpleEntry<Map<String, PointValues>, Set<String>> indicesAndAbsent;
        private IndexSearcher pointIndexSearcher;

        public AbstractIndexConsumer(LeafReaderContext toContext) throws IOException {
            this.toContext = toContext;
            pointIndexSearcher = joinIndexQuery.indexManager.acquire();
        }

        void walkFromContexts(LeafReaderContext toContext) throws IOException {
            indexPointsNames = new LinkedHashMap<>(fromLeaves.size());
            String toSegmentName = JoinIndexHelper.getSegmentName(toContext);
            // TODO approximate via sibling pages
            //nextFromLeaf:
            for (JoinIndexHelper.FromContextCache fromLeaf : fromLeaves) {
                String fromSegmentName = JoinIndexHelper.getSegmentName(fromLeaf.lrc);
                String indexFieldName = JoinIndexHelper.getPointIndexFieldName(fromSegmentName, toSegmentName);
                indexPointsNames.put(indexFieldName, fromLeaf);
            }
            this.indicesAndAbsent = extractIndices(pointIndexSearcher, indexPointsNames.keySet());
        }

        void prepareEager() throws IOException {
            for (Map.Entry<String, PointValues> joinIndexByName : indicesAndAbsent.getKey().entrySet()) {
                JoinIndexHelper.FromContextCache fromCtxLeaf = indexPointsNames.get(joinIndexByName.getKey());
                this.onIndexPage(fromCtxLeaf, joinIndexByName.getValue());
            }
            for (String absentIndexName : indicesAndAbsent.getValue()) {
                JoinIndexHelper.FromContextCache fromCtxLeaf = indexPointsNames.get(absentIndexName);
                JoinIndexHelper.indexJoinSegments(
                        joinIndexQuery.indexManager, joinIndexQuery.writerFactory,
                        fromCtxLeaf.lrc.reader().getSortedSetDocValues(joinIndexQuery.fromField),
                        toContext.reader().getSortedSetDocValues(joinIndexQuery.toField),
                        absentIndexName,
                        this.createTupleConsumer(fromCtxLeaf));
            }
        }

        @Override
        public void close() throws Exception {
            joinIndexQuery.indexManager.release(pointIndexSearcher);
            pointIndexSearcher = null;
        }
    }

    private class EagerIndexConsumer extends AbstractIndexConsumer {
        private final FixedBitSet toBits;

        public EagerIndexConsumer(LeafReaderContext toContext) throws IOException {
            super(toContext);
            toBits = new FixedBitSet(toContext.reader().maxDoc());
        }

        @Override
        public void onIndexPage(JoinIndexHelper.FromContextCache fromCtx, PointValues indexPoints) throws IOException {
            indexPoints.intersect(new JoinIndexHelper.InnerJoinVisitor(fromCtx.bits, toBits,
                    fromCtx.lowerDocId, fromCtx.upperDocId));
        }

        @Override
        public IntBinaryOperator createTupleConsumer(JoinIndexHelper.FromContextCache fromLeaf) {
            return (f, t) -> {
                if (f >= fromLeaf.lowerDocId && f <= fromLeaf.upperDocId && fromLeaf.bits.get(f)) {
                    toBits.set(t);
                }
                return 0;
            };
        }

        public ScorerSupplier createEager() {
            if(toBits.scanIsEmpty())
                return null;
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

        public void prepareLazy() {
            throw new UnsupportedOperationException();
        }

        public ScorerSupplier createLazy() {
            throw new UnsupportedOperationException();
        }
    }
}
