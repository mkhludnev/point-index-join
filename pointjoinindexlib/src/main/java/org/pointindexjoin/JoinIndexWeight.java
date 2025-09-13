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
import java.util.LinkedHashMap;
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
        SingleToSegProcessor toSegments = this.extractIndices(joinIndexQuery.fromSearcher.getIndexReader().leaves(),
                joinIndexQuery.indexManager,
                toContext,
                joinIndexQuery.fromField,
                joinIndexQuery.toField,
                fromLeaves
        );
        SingleToSegProcessor joinConsumer = toSegments;//[0];
        return joinConsumer.createExactBitsFromAbsentSegs(joinIndexQuery.writerFactory);
         // if (joinConsumer.isFullyIndexed()) {
         //     return joinConsumer.createLazy();
         // } else {
         //     return joinConsumer.createEager(joinIndexQuery.writerFactory);
         // }
    }

    //TODO simplify to single toSegment
    SingleToSegProcessor extractIndices(List<LeafReaderContext> fromSegments, SearcherManager pointIndexManager, LeafReaderContext toSegment, String fromField, String toField, List<JoinIndexHelper.FromContextCache> fromLeavesCached) throws IOException {

        // 1. Build mapping from point index name to (fromLeaf, toLeaf) pair
        Map<String, Map.Entry<LeafReaderContext, LeafReaderContext>> pointIndexNameToPair = new HashMap<>();
        Map<String, SingleToSegProcessor.FromSegIndexData> dataByJoinIndexByFieldName = new LinkedHashMap<>(fromSegments.size());
        String toSegmentName = getSegmentName(toSegment);
        //List<SingleToSegProcessor.FromSegIndexData> fromSegData = new ArrayList<>(fromSegments.size());
        for (LeafReaderContext fromLeaf : fromSegments) {
            String fromSegmentName = getSegmentName(fromLeaf);
            String pointIndexName = getPointIndexFieldName(fromSegmentName, toSegmentName);
            pointIndexNameToPair.put(pointIndexName, Map.entry(fromLeaf, toSegment));
            SingleToSegProcessor.FromSegIndexData fromSegIndexData = new SingleToSegProcessor.FromSegIndexData(pointIndexName, fromLeaf);
            //fromSegData.add(fromSegIndexData);
            dataByJoinIndexByFieldName.put(pointIndexName, fromSegIndexData);
        }

        // 2. Prepare result containers
        int fromSize = fromSegments.size();//toSegments.size();
        //for (int i = 0; i < toSize; i++) {
        PointValues[] indicesByTo = new PointValues[fromSize];
        String[] absentPointsByTo = new String[fromSize];
        //}

        // 3. Mark all as absent initially
        for (var entry : pointIndexNameToPair.entrySet()) {
            int fromOrd = entry.getValue().getKey().ord;
            //int toOrd = 0;// entry.getValue().getValue().ord;
            absentPointsByTo[fromOrd] = entry.getKey();
        }

        List<SingleToSegProcessor.FromSegIndexData> toProcess = new ArrayList<>(fromSize);
        // 4. Fill found indices, clear absent markers
        IndexSearcher searcher = pointIndexManager.acquire();
        for (LeafReaderContext pointSegment : searcher.getIndexReader().leaves()) {
            FieldInfos fieldInfos = pointSegment.reader().getFieldInfos();
            for (FieldInfo fieldInfo : fieldInfos) {
                SingleToSegProcessor.FromSegIndexData fromSegIndexData = dataByJoinIndexByFieldName.remove(fieldInfo.name);
                Map.Entry<LeafReaderContext, LeafReaderContext> pair = pointIndexNameToPair.get(fieldInfo.name);
                if (pair != null) {
                    int fromOrd = pair.getKey().ord;
                    //int toOrd = 0;//pair.getValue().ord;
                    if (fieldInfo.getPointIndexDimensionCount() == 2) {
                        indicesByTo//.get(toOrd)
                                [fromOrd] = pointSegment.reader().getPointValues(fieldInfo.name);
                        fromSegIndexData.joinValues = pointSegment.reader().getPointValues(fieldInfo.name);
                        toProcess.add(fromSegIndexData);
                    }// else tombstone
                    //dataByJoinIndexByFieldName.remove(fieldInfo.name);
                    absentPointsByTo//.get(toOrd)
                            [fromOrd] = null; // Clear absent marker

                }//TODO else drop redundant
            }
        }

        this.closeHook.accept(()->pointIndexManager.release(searcher));

        // 5. Build processors for each to-segment
        SingleToSegProcessor processors;// = new SingleToSegProcessor[toSize];
        //for (int toOrd = 0; toOrd < toSize; toOrd++) {
            processors//[toOrd]
                    = new SingleToSegProcessor(
                    fromField, toField, pointIndexManager, fromLeavesCached, toSegment,//s.get(toOrd),
                    toProcess//.get(0)
                    , dataByJoinIndexByFieldName.values()//absentPointsByTo//.get(0)
            );
        //}
        return processors;
    }
    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

}
