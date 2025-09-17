package org.pointindexjoin;

import static org.pointindexjoin.JoinIndexHelper.getPointIndexFieldName;
import static org.pointindexjoin.JoinIndexHelper.getSegmentName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

class JoinIndexWeight extends Weight {

    private final JoinIndexQuery joinIndexQuery;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;
    private final Consumer<AutoCloseable> closeHook;
    //private final SingleToSegProcessor[] toSegments;
    //private final SingleToSegProcessor[] toSegments;

    public JoinIndexWeight(JoinIndexQuery joinIndexQuery, ScoreMode scoreMode, Consumer<AutoCloseable> consumer)
            throws IOException {
        super(joinIndexQuery);
        this.joinIndexQuery = joinIndexQuery;
        // this.scoreMode = scoreMode;
        this.closeHook = consumer;
        this.fromLeaves = joinIndexQuery.cacheFromQuery();
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
        SingleToSegProcessor processor = this.extractIndices(
                toContext);
        return processor.createScorerSupplier(joinIndexQuery.writerFactory);
    }

    SingleToSegProcessor extractIndices(LeafReaderContext toSegment) throws IOException {

        Map<String, SingleToSegProcessor.FromSegIndexData> needsToBeIndexed = new LinkedHashMap<>(fromLeaves.size());
        String toSegmentName = getSegmentName(toSegment);
        //List<SingleToSegProcessor.FromSegIndexData> fromSegData = new ArrayList<>(fromSegments.size());
        for (JoinIndexHelper.FromContextCache fromLeaf : fromLeaves) {
            String fromSegmentName = getSegmentName(fromLeaf.lrc);
            String pointIndexName = getPointIndexFieldName(fromSegmentName, toSegmentName);
            SingleToSegProcessor.FromSegIndexData fromSegIndexData = new SingleToSegProcessor.FromSegIndexData(pointIndexName,
                    fromLeaf);
            needsToBeIndexed.put(pointIndexName, fromSegIndexData);
        }

        List<SingleToSegProcessor.FromSegIndexData> existingIndices = new ArrayList<>(fromLeaves.size());
        IndexSearcher searcher = joinIndexQuery.indexManager.acquire();
        for (LeafReaderContext pointSegment : searcher.getIndexReader().leaves()) {
            FieldInfos fieldInfos = pointSegment.reader().getFieldInfos();
            for (FieldInfo fieldInfo : fieldInfos) {
                SingleToSegProcessor.FromSegIndexData fromSegIndexData = needsToBeIndexed.remove(fieldInfo.name);
                if (fromSegIndexData != null) {
                    if (fieldInfo.getPointIndexDimensionCount() == 2) {
                        fromSegIndexData.joinValues = pointSegment.reader().getPointValues(fieldInfo.name);
                        existingIndices.add(fromSegIndexData);
                    }// else tombstone, we don't care. this from query doesn't hit that seg.
                }
            }
        }
        if (!existingIndices.isEmpty()) {
            this.closeHook.accept(() -> joinIndexQuery.indexManager.release(searcher));
        } else {
            joinIndexQuery.indexManager.release(searcher);
        }

        SingleToSegProcessor processor;// = new SingleToSegProcessor[toSize];
        processor//[toOrd]
                = new SingleToSegProcessor(
                joinIndexQuery.fromField, joinIndexQuery.toField, joinIndexQuery.indexManager, toSegment,
//s.get(toOrd),
                existingIndices//.get(0)
                , needsToBeIndexed.values()//absentPointsByTo//.get(0)
        );
        return processor;
    }
    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

}
