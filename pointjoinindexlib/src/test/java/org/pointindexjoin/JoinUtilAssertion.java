package org.pointindexjoin;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.util.BytesRef;

class JoinUtilAssertion extends JoinAssertion {
    public JoinUtilAssertion(List<String> selectedChildIds,
                             Map<String, String> childToParentMap,
                             IndexSearcher fromSearcher,
                             SearcherManager indexManager,
                             Supplier<IndexWriter> indexWriterSupplier,
                             IndexSearcher toSearcher, Random random) {
        super(selectedChildIds, childToParentMap, fromSearcher, indexManager,
                indexWriterSupplier, toSearcher, random);
    }

    @Override
    protected Query createCoreJoinQuery() throws IOException {
        return JoinUtil.createJoinQuery(
                "fk", true, "id",
                new TermInSetQuery("id", selectedChildIds.stream().map(BytesRef::new).toList()),
                fromSearcher,
                org.apache.lucene.search.join.ScoreMode.None
        );
    }
}
