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
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Weight;

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
                joinIndexQuery.indexManager,
                toContext,
                joinIndexQuery.fromField,
                joinIndexQuery.toField,
                fromLeaves
        );
        return processor.createScorerSupplier(joinIndexQuery.writerFactory);
    }

    SingleToSegProcessor extractIndices(SearcherManager pointIndexManager, LeafReaderContext toSegment, String fromField, String toField, List<JoinIndexHelper.FromContextCache> fromLeavesCached) throws IOException {

        Map<String, SingleToSegProcessor.FromSegIndexData> needsToBeIndexed = new LinkedHashMap<>(fromLeavesCached.size());
        String toSegmentName = getSegmentName(toSegment);
        //List<SingleToSegProcessor.FromSegIndexData> fromSegData = new ArrayList<>(fromSegments.size());
        for (JoinIndexHelper.FromContextCache fromLeaf : fromLeavesCached) {
            String fromSegmentName = getSegmentName(fromLeaf.lrc);
            String pointIndexName = getPointIndexFieldName(fromSegmentName, toSegmentName);
            SingleToSegProcessor.FromSegIndexData fromSegIndexData = new SingleToSegProcessor.FromSegIndexData(pointIndexName,
                    fromLeaf);
            needsToBeIndexed.put(pointIndexName, fromSegIndexData);
        }

        List<SingleToSegProcessor.FromSegIndexData> existingIndices = new ArrayList<>(fromLeavesCached.size());
        IndexSearcher searcher = pointIndexManager.acquire();
        // TODO, nice to have idxSeg[fieldName]???
        for (LeafReaderContext pointSegment : searcher.getIndexReader().leaves()) {
            FieldInfos fieldInfos = pointSegment.reader().getFieldInfos();
            for (FieldInfo fieldInfo : fieldInfos) {
                SingleToSegProcessor.FromSegIndexData fromSegIndexData = needsToBeIndexed.remove(fieldInfo.name);
                if (fromSegIndexData != null) {
                    if (fieldInfo.getPointIndexDimensionCount() == 2) {
                        fromSegIndexData.joinValues = pointSegment.reader().getPointValues(fieldInfo.name);
                        existingIndices.add(fromSegIndexData);
                    }// else tombstone, we don't care. this from query doesn't hit that seg.
                }//TODO else drop redundant
            }
        }
        this.closeHook.accept(()->pointIndexManager.release(searcher));

        SingleToSegProcessor processor;// = new SingleToSegProcessor[toSize];
        processor//[toOrd]
                = new SingleToSegProcessor(
                fromField, toField, pointIndexManager, fromLeavesCached, toSegment,//s.get(toOrd),
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
