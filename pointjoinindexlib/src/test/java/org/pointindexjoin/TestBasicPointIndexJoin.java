package org.pointindexjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.BeforeClass;

// import com.carrotsearch.randomizedtesting.annotations.Seed;
//
// @Seed("30CB661418D2B6E5")
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "nope")
public class TestBasicPointIndexJoin extends LuceneTestCase {

    public static final Logger LOGGER = Logger.getLogger(TestBasicPointIndexJoin.class.getCanonicalName());
    public static final int PARENTS = 10000;
    public static final int CHILD_PER_PARENT = 200;
    public static final int REPEATS = 100;
    private static Directory parentDir;
    private static Directory childDir;
    private static IndexWriter parentWriter;
    private static IndexWriter childWriter;
    private static IndexSearcher parentSearcher;
    private static IndexSearcher childSearcher;
    private static int orphanCnt;
    private static Map<String, String> childToParentMap;

    @BeforeClass
    public static void index() throws IOException {
        parentDir = newDirectory();
        childDir = newDirectory();

        parentWriter = new IndexWriter(parentDir,
                new IndexWriterConfig()
                //                .setInfoStream(new JavaLoggingInfoStream(Level.INFO))
        );
        childWriter = new IndexWriter(childDir,
                new IndexWriterConfig()//.setInfoStream(new JavaLoggingInfoStream(Level.INFO))
        );

        // Map to track child ID to parent ID
        childToParentMap = new HashMap<>();
        orphanCnt = 0;
        String orphanId = null;
        for (int parentId = 1; parentId <= PARENTS; parentId++) {
            String parentIdStr = String.valueOf(parentId);
            indexParent(parentIdStr, parentWriter);

            if (rarely()) {
                parentWriter.commit();
            }

            for (int childNum = 1; childNum <= CHILD_PER_PARENT; childNum++) {
                String childId = parentIdStr + "_" + childNum; // Unique child ID
                indexChild(childWriter, parentIdStr, childId);
                childToParentMap.put(childId, parentIdStr);
                if (rarely() && rarely()) {
                    indexChild(childWriter, null, orphanId = ("orphan_" + (orphanCnt++)));
                    childToParentMap.put(orphanId, null);
                }
                if (rarely()) {
                    childWriter.commit();
                }

            }
        }

        parentWriter.commit();
        childWriter.commit();


        parentSearcher = new IndexSearcher(DirectoryReader.open(parentDir));
        childSearcher = new IndexSearcher(DirectoryReader.open(childDir));
    }


    private static void indexParent(String id, IndexWriter w) throws IOException {
        Document parent1 = new Document();
        parent1.add(new SortedSetDocValuesField("id", new BytesRef(id)));
        parent1.add(new StringField("id", id, Field.Store.YES));
        w.updateDocument(new Term("id", id), parent1);
    }

    private static void indexChild(IndexWriter fromw, String fk, String id) throws IOException {
        Document child1 = new Document();
        if (fk!=null) {
            child1.add(new SortedSetDocValuesField("fk", new BytesRef(fk)));
        }
        child1.add(new StringField("id", id, Field.Store.YES));
        fromw.updateDocument(new Term("id", id), child1);
    }

    /*
        private static void assertJoin(List<String> selectedChildIds, Map<String, String> childToParentMap,
                                       IndexSearcher fromSearcher, SearcherManager indexManager,
                                       Supplier<IndexWriter> indexWriterSupplier, IndexSearcher toSearcher) throws Exception {
            LOGGER.
                    info("children:" + selectedChildIds);
            Set<String> parentsExpected = selectedChildIds.stream().map(childToParentMap::get).filter(p->p!=null).collect
            (Collectors.toSet());
            LOGGER.
                    info("parents expected:" + parentsExpected);
            String removeParent = null;
            if (!parentsExpected.isEmpty() && random().nextBoolean()) {
                Iterator<String> iterator = parentsExpected.iterator();
                removeParent = iterator.next();
                iterator.remove();
                LOGGER.
                        info("removed parent:"+removeParent+". Parents expected "+parentsExpected);
            }
            JoinIndexQuery joinIndexQuery = new JoinIndexQuery(fromSearcher,
                    new TermInSetQuery("id", selectedChildIds.stream().map(BytesRef::new).toList()),
                    "fk", "id", indexManager,
                    indexWriterSupplier);
            Query join = joinIndexQuery;
            if (removeParent != null) {
                join = new BooleanQuery.Builder().add(join, BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("id", removeParent)), BooleanClause.Occur.MUST_NOT).build();
            }
            TopDocs search = toSearcher.search(join, selectedChildIds.size());
            assertEquals(
                    selectedChildIds + " should join to " + parentsExpected + " but actually " +
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
            joinIndexQuery.close();
        }
    */
    // @Seed("9B317B01C70E30CB")
    public void testBasic() throws Exception {
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
        long indexJoin = 0, queryJoin = 0;
        for (int pass = 0; pass < CHILD_PER_PARENT; pass++) {
            List<String> childIds = new ArrayList<>(childToParentMap.keySet());
            if (orphanCnt > 0) {
                childIds.add("orphan_" + (random().nextInt(orphanCnt)));
            }
            Collections.shuffle(childIds, random());
            List<String> selectedChildIds = childIds.subList(0, 10);
            {
                //assertJoin(selectedChildIds, childToParentMap, childSearcher, indexManager, indexWriterSupplier,
                // parentSearcher);
                for (int r = 0; r < REPEATS; r++) {
                    indexJoin += new JoinAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
                            indexWriterSupplier, parentSearcher,
                            random()).assertJoin().getAsLong();

                    /*queryJoin += new JoinUtilAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
                            indexWriterSupplier,
                            parentSearcher,
                            random()).assertJoin().getAsLong();*/
                }
                LOGGER.info("passed " + pass + " bare search");
            }
            continue;
            /**
            {
                Collections.shuffle(childIds, random());
                String childToRemove = selectedChildIds.getFirst();

             childSearcher.getIndexReader().close();
             childWriter.deleteDocuments(new Term("id", childToRemove));
             childWriter.commit();
             childSearcher = new IndexSearcher(DirectoryReader.open(childDir));
                childToParentMap.remove(childToRemove);
                LOGGER.info("remove child " + childToRemove);
             for (int r=0;r<REPEATS;r++) {
             indexJoin += new JoinAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
             indexWriterSupplier, parentSearcher,
             random()).assertJoin().getAsLong();
             queryJoin += new JoinUtilAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
             indexWriterSupplier, parentSearcher,
             random()).assertJoin().getAsLong();
             }
                LOGGER.info("passed " + pass + "child remove");
            }
            {
                List<String> parentsExpected =
                        selectedChildIds.stream().map(childToParentMap::get).filter(p -> p != null).collect(Collectors.toList());
                Collections.shuffle(parentsExpected, random());
                String removeParent = parentsExpected.getFirst();
                for (Iterator<Map.Entry<String, String>> entries = childToParentMap.entrySet().iterator(); entries.hasNext(); ) {
                    Map.Entry parentByChild = entries.next();
                    if (parentByChild.getValue() != null && parentByChild.getValue().equals(removeParent)) {
                        entries.remove();
                    }
                }
             parentSearcher.getIndexReader().close();
             parentWriter.deleteDocuments(new Term("id", removeParent));
             parentWriter.commit();
             parentSearcher = new IndexSearcher(DirectoryReader.open(parentDir));
                LOGGER.info("removed parent " + removeParent);
             for (int r=0;r<REPEATS;r++) {
             indexJoin += new JoinAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
             indexWriterSupplier, parentSearcher,
             random()).assertJoin().getAsLong();
             queryJoin += new JoinUtilAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
             indexWriterSupplier, parentSearcher,
             random()).assertJoin().getAsLong();
             }
                LOGGER.info("passed " + pass + "parent remove");
            }
            {
             int parentId = random().nextInt((int) (PARENTS *0.9), (int) (CHILD_PER_PARENT * 1.1));
                String parentIdStr = String.valueOf(parentId);
             indexParent(parentIdStr, parentWriter);
                //if (rarely()) {
             parentWriter.commit();
                //}
             String childId = parentIdStr + "_" + random().nextInt(CHILD_PER_PARENT);
             indexChild(childWriter, parentIdStr, childId);
                childToParentMap.put(childId, parentIdStr);
                //if (rarely()) {
             childWriter.commit();
                //}
             childSearcher.getIndexReader().close();
             childSearcher = new IndexSearcher(DirectoryReader.open(childDir));
             parentSearcher.getIndexReader().close();
             parentSearcher = new IndexSearcher(DirectoryReader.open(parentDir));
             selectedChildIds.add(childId);
             LOGGER.info("added child & parent " + childId);
             for (int r=0;r<REPEATS;r++) {
             indexJoin += new JoinAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
             indexWriterSupplier, parentSearcher,
             random()).assertJoin().getAsLong();
             queryJoin += new JoinUtilAssertion(selectedChildIds, childToParentMap, childSearcher, indexManager,
             indexWriterSupplier, parentSearcher,
             random()).assertJoin().getAsLong();
             }
             LOGGER.info("passed " + pass + " add/upd both");
             }
             */
        }
        LOGGER.info("index time " + indexJoin + "ms; q-time " + queryJoin + "ms");

        indexManager.close();
        parentSearcher.getIndexReader().close();
        childSearcher.getIndexReader().close();
        parentWriter.close();

        parentDir.close();

        childWriter.close();
        childDir.close();
        indexManager.close();
        joinindexdir.close();
    }

}
