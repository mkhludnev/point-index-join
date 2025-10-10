package org.pointindexjoin;

import static org.pointindexjoin.JoinIndexHelper.getPointIndexFieldName;
import static org.pointindexjoin.JoinIndexHelper.getSegmentName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

class JoinIndexWeight2 extends Weight {

    private final JoinIndexQuery2 joinIndexQuery;
    private final List<JoinIndexHelper.FromContextCache> fromLeaves;
    private final Consumer<AutoCloseable> closeHook;

    public JoinIndexWeight2(JoinIndexQuery2 joinIndexQuery, ScoreMode scoreMode, Consumer<AutoCloseable> consumer)
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
        SingleToSegDVProcessor processor = this.extractIndices(
                toContext);
        return processor.createScorerSupplier(joinIndexQuery.writerFactory);
    }

    SingleToSegDVProcessor extractIndices(LeafReaderContext toSegment) throws IOException {

        Map<String, SingleToSegDVProcessor.FromSegDocValuesData> needsToBeIndexed = new LinkedHashMap<>(fromLeaves.size());
        String toSegmentName = getSegmentName(toSegment);
        //List<SingleToSegProcessor.FromSegIndexData> fromSegData = new ArrayList<>(fromSegments.size());
        for (JoinIndexHelper.FromContextCache fromLeaf : fromLeaves) {
            String fromSegmentName = getSegmentName(fromLeaf.lrc);
            String pointIndexName = getPointIndexFieldName(fromSegmentName, toSegmentName);
            SingleToSegDVProcessor.FromSegDocValuesData fromSegIndexData = new SingleToSegDVProcessor.FromSegDocValuesData(pointIndexName,
                    fromLeaf);
            needsToBeIndexed.put(pointIndexName, fromSegIndexData);
        }

        List<SingleToSegDVProcessor.FromSegDocValuesData> existingIndices = new ArrayList<>(fromLeaves.size());
        IndexSearcher searcher = joinIndexQuery.indexManager.acquire();
        Logger.getLogger(getClass().getCanonicalName()).info(""+searcher.getIndexReader().getContext().leaves().stream().map(l-> dump(
                l)).collect(
                Collectors.joining("; ")));
        for (LeafReaderContext pointSegment : searcher.getIndexReader().leaves()) {
            FieldInfos fieldInfos = pointSegment.reader().getFieldInfos();
            for (FieldInfo fieldInfo : fieldInfos) {
                SingleToSegDVProcessor.FromSegDocValuesData fromSegIndexData = needsToBeIndexed.remove(fieldInfo.name);
                if (fromSegIndexData != null) {
                    if (isSuitableFieldType(fieldInfo)) {
                        fromSegIndexData.toDocsByFrom = pointSegment.reader().getSortedNumericDocValues(fieldInfo.name);
                        fromSegIndexData.maxToDocIndexed = pointSegment.reader().maxDoc();
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

        SingleToSegDVProcessor processor;// = new SingleToSegProcessor[toSize];
        processor//[toOrd]
                = new SingleToSegDVProcessor(
                joinIndexQuery.fromField, joinIndexQuery.toField, joinIndexQuery.indexManager, toSegment,
//s.get(toOrd),
                existingIndices//.get(0)
                , needsToBeIndexed.values()//absentPointsByTo//.get(0)
        );
        return processor;
    }

    private static String dump(LeafReaderContext l) {
        StringBuilder append = new StringBuilder().append(((SegmentReader) l.reader()).getSegmentName())
                .append(":")
                .append(((SegmentReader) l.reader()).maxDoc()).append("[");
        l.reader().getFieldInfos().forEach(fi -> append.append(fi.getName()).append(","));
        return append.append("]").toString();
    }

    private static boolean isSuitableFieldType(FieldInfo fieldInfo) {
        return fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

}
