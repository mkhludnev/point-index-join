package org.pointindexjoin;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

//@Seed("67DC4AA556A66043")
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "nope")
public class TestBasicPointIndexJoin extends LuceneTestCase {
    private static void indexParent(String id, IndexWriter w) throws IOException {
        Document parent1 = new Document();
        parent1.add(new SortedSetDocValuesField("id", new BytesRef(id)));
        parent1.add(new StringField("id", id, Field.Store.YES));
        w.addDocument(parent1);
    }

    private static void indexChild(IndexWriter fromw, String fk, String id) throws IOException {
        Document child1 = new Document();
        child1.add(new SortedSetDocValuesField("fk", new BytesRef(fk)));
        child1.add(new StringField("id", id, Field.Store.YES));
        fromw.addDocument(child1);
    }

    private static void assertJoin(List<String> selectedChildIds, Map<String, String> childToParentMap, IndexSearcher fromSearcher, SearcherManager indexManager, Supplier<IndexWriter> indexWriterSupplier, IndexSearcher toSearcher) throws IOException {
        Set<String> parentsExpected = selectedChildIds.stream().map(childToParentMap::get).collect(Collectors.toSet());
        String removeParent = null;
        if (!parentsExpected.isEmpty() && random().nextBoolean()) {
            Iterator<String> iterator = parentsExpected.iterator();
            removeParent = iterator.next();
            iterator.remove();
        }
        Query join = new JoinIndexQuery(fromSearcher,
                new TermInSetQuery("id", selectedChildIds.stream().map(BytesRef::new).toList()),
                "fk", "id", indexManager,
                indexWriterSupplier);
        if (removeParent != null) {
            join = new BooleanQuery.Builder().add(join, BooleanClause.Occur.MUST)
                    .add(new TermQuery(new Term("id", removeParent)), BooleanClause.Occur.MUST_NOT).build();
        }
        TopDocs search = toSearcher.search(join, selectedChildIds.size());
        assertEquals(
                selectedChildIds + " yields " + parentsExpected + " but " +
                        Arrays.stream(search.scoreDocs, 0, (int) search.totalHits.value())
                                .map(sd -> {
                                    try {
                                        return toSearcher.storedFields().document(sd.doc).get("id");
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }).collect(Collectors.joining(","))
                , parentsExpected.size(), search.totalHits.value());

        for (ScoreDoc doc : search.scoreDocs) {
            Document document = toSearcher.storedFields().document(doc.doc);
            assertTrue(document.get("id"), parentsExpected.remove(document.get("id")));
        }
        assertTrue(parentsExpected.isEmpty());
    }

    //@Seed("CC2657B0897AE66D")
    public void testBasic() throws IOException {
        Directory dir = newDirectory();
        Directory fromDir = newDirectory();

        IndexWriter w = new IndexWriter(dir,
                new IndexWriterConfig()
                //                .setInfoStream(new JavaLoggingInfoStream(Level.INFO))
        );
        IndexWriter fromw = new IndexWriter(fromDir,
                new IndexWriterConfig()//.setInfoStream(new JavaLoggingInfoStream(Level.INFO))
        );

        // Map to track child ID to parent ID
        Map<String, String> childToParentMap = new HashMap<>();

        for (int parentId = 1; parentId <= 1000; parentId++) {
            String parentIdStr = String.valueOf(parentId);
            indexParent(parentIdStr, w);

            if (rarely()) {
                w.commit();
            }

            for (int childNum = 1; childNum <= 100; childNum++) {
                String childId = parentIdStr + "_" + childNum; // Unique child ID
                indexChild(fromw, parentIdStr, childId);
                childToParentMap.put(childId, parentIdStr);
                if (rarely()) {
                    fromw.commit();
                }
            }
        }

        w.commit();
        fromw.commit();

        w.close();
        fromw.close();

        IndexSearcher toSearcher = new IndexSearcher(DirectoryReader.open(dir));
        IndexSearcher fromSearcher = new IndexSearcher(DirectoryReader.open(fromDir));


        Directory joinindexdir = newDirectory();
        IndexWriter idxW = new IndexWriter(joinindexdir, new IndexWriterConfig());
        idxW.close();

        Supplier<IndexWriter> indexWriterSupplier = () -> {
            try {
                return new IndexWriter(joinindexdir, newIndexWriterConfig());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        SearcherManager indexManager = new SearcherManager(joinindexdir, null);

        //TopDocs parentResult = toSearcher.search(SortedSetDocValuesField.newSlowExactQuery("id", new BytesRef("639")), 10);
        //System.out.println(parentResult);
        //assertJoin(Arrays.asList("635_39"), childToParentMap, fromSearcher, indexManager, indexWriterSupplier, toSearcher);
        for (int pass = 0; pass < 10; pass++) {
            List<String> childIds = new ArrayList<>(childToParentMap.keySet());
            Collections.shuffle(childIds, random());
            List<String> selectedChildIds = childIds.subList(0, 10);
            assertJoin(selectedChildIds, childToParentMap, fromSearcher, indexManager, indexWriterSupplier, toSearcher);
        }

        indexManager.close();
        toSearcher.getIndexReader().close();
        fromSearcher.getIndexReader().close();
        dir.close();
        fromDir.close();
        joinindexdir.close();
    }
}
