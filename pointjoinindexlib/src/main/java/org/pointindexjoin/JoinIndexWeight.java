package org.pointindexjoin;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.pointindexjoin.JoinIndexHelper.getPointIndexFieldName;
import static org.pointindexjoin.JoinIndexHelper.getSegmentName;

class JoinIndexWeight extends Weight {

    private final JoinIndexQuery joinIndexQuery;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;
    private final Consumer<AutoCloseable> closeHook;
    //private final SingleToSegProcessor[] toSegments;
    //private final SingleToSegProcessor[] toSegments;

    public JoinIndexWeight(IndexSearcher toSearcher, JoinIndexQuery joinIndexQuery, ScoreMode scoreMode, Consumer<AutoCloseable> consumer) throws IOException {
        super(joinIndexQuery);
        this.joinIndexQuery = joinIndexQuery;
        // this.scoreMode = scoreMode;
        this.closeHook = consumer;
        this.fromLeaves = joinIndexQuery.cacheFromQuery(); // TODO defer it even further
//        this.toSegments = JoinIndexHelper.extractIndices(joinIndexQuery.fromSearcher.getIndexReader().leaves(),
//                joinIndexQuery.indexManager,
//                toSearcher.getIndexReader().leaves(),
//                joinIndexQuery.fromField,
//                joinIndexQuery.toField,
//                fromLeaves
//        );
        //this.toSegments = toSegProcss.toArray(new SingleToSegProcessor[]{});
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
        SingleToSegProcessor[] toSegments = this.extractIndices(joinIndexQuery.fromSearcher.getIndexReader().leaves(),
                joinIndexQuery.indexManager,
                toContext,
                joinIndexQuery.fromField,
                joinIndexQuery.toField,
                fromLeaves
        );
        SingleToSegProcessor joinConsumer = toSegments[0];
        return joinConsumer.createExactBitsFromAbsentSegs(joinIndexQuery.writerFactory);
         // if (joinConsumer.isFullyIndexed()) {
         //     return joinConsumer.createLazy();
         // } else {
         //     return joinConsumer.createEager(joinIndexQuery.writerFactory);
         // }
    }

    //TODO simplify to single toSegment
    SingleToSegProcessor[] extractIndices(List<LeafReaderContext> fromSegments, SearcherManager pointIndexManager, LeafReaderContext toSegment, String fromField, String toField, List<JoinIndexHelper.FromContextCache> fromLeavesCached) throws IOException {

        // 1. Build mapping from point index name to (fromLeaf, toLeaf) pair
        Map<String, Map.Entry<LeafReaderContext, LeafReaderContext>> pointIndexNameToPair = new HashMap<>();
        for (LeafReaderContext fromLeaf : fromSegments) {
            for (LeafReaderContext toLeaf : List.of(toSegment)) {
                String pointIndexName = getPointIndexFieldName(getSegmentName(fromLeaf), getSegmentName(toLeaf));
                pointIndexNameToPair.put(pointIndexName, Map.entry(fromLeaf, toLeaf));
            }
        }

        // 2. Prepare result containers
        int fromSize = fromSegments.size(), toSize = 1;//toSegments.size();
        List<PointValues[]> indicesByTo = new ArrayList<>(toSize);
        List<String[]> absentPointsByTo = new ArrayList<>(toSize);
        for (int i = 0; i < toSize; i++) {
            indicesByTo.add(new PointValues[fromSize]);
            absentPointsByTo.add(new String[fromSize]);
        }

        // 3. Mark all as absent initially
        for (var entry : pointIndexNameToPair.entrySet()) {
            int fromOrd = entry.getValue().getKey().ord;
            int toOrd = 0;// entry.getValue().getValue().ord;
            absentPointsByTo.get(toOrd )[fromOrd] = entry.getKey();
        }

        // 4. Fill found indices, clear absent markers
        IndexSearcher searcher = pointIndexManager.acquire();
        for (LeafReaderContext pointSegment : searcher.getIndexReader().leaves()) {
            FieldInfos fieldInfos = pointSegment.reader().getFieldInfos();
            for (FieldInfo fieldInfo : fieldInfos) {
                Map.Entry<LeafReaderContext, LeafReaderContext> pair = pointIndexNameToPair.get(fieldInfo.name);
                if (pair != null) {
                    int fromOrd = pair.getKey().ord;
                    int toOrd = 0;//pair.getValue().ord;
                    if (fieldInfo.getPointDimensionCount() == 2) {
                        indicesByTo.get(toOrd)[fromOrd] = pointSegment.reader().getPointValues(fieldInfo.name);
                    }// else tombstone
                    absentPointsByTo.get(toOrd)[fromOrd] = null; // Clear absent marker
                }//TODO else drop redundant
            }
        }

        this.closeHook.accept(()->pointIndexManager.release(searcher));

        // 5. Build processors for each to-segment
        SingleToSegProcessor[] processors = new SingleToSegProcessor[toSize];
        for (int toOrd = 0; toOrd < toSize; toOrd++) {
            processors[toOrd] = new SingleToSegProcessor(
                    fromField, toField, pointIndexManager, fromLeavesCached, toSegment,//s.get(toOrd),
                    indicesByTo.get(toOrd), absentPointsByTo.get(toOrd)
            );
        }
        return processors;
    }
    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

}
