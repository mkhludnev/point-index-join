package org.pointindexjoin;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.function.Supplier;

public class TestBasicPointIndexJoin extends LuceneTestCase {
    private static void indexParent(String id, RandomIndexWriter w) throws IOException {
        Document parent1 = new Document();
        parent1.add(new SortedSetDocValuesField("id", new BytesRef(id)));
        w.addDocument(parent1);
    }

    private static void indexChild(RandomIndexWriter fromw, String fk, String id) throws IOException {
        Document child1 = new Document();
        child1.add(new SortedSetDocValuesField("fk", new BytesRef(fk)));
        child1.add(new StringField("id", id, Field.Store.YES));
        fromw.addDocument(child1);
    }

    public void testBasic() throws IOException {
        Directory dir = newDirectory();
        Directory fromDir = newDirectory();

        RandomIndexWriter w =
                new RandomIndexWriter(
                        random(),
                        dir,
                        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
        RandomIndexWriter fromw =
                new RandomIndexWriter(
                        random(),
                        fromDir,
                        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

        indexParent("1", w);
        indexChild(fromw, "1", "11");
        indexChild(fromw, "1", "12");

        indexParent("2", w);
        indexChild(fromw, "2", "21");
        indexChild(fromw, "2", "22");

        w.commit();
        fromw.commit();

        IndexSearcher toSearcher = new IndexSearcher(w.getReader());
        IndexSearcher fromSearcher = new IndexSearcher(fromw.getReader());

        w.close();
        fromw.close();

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
        {
            Query join = new JoinIndexQuery(fromSearcher, new TermQuery(new Term("id", "22")),
                    "fk", "id", indexManager,
                    indexWriterSupplier);
            TopDocs search = toSearcher.search(join, toSearcher.getIndexReader().maxDoc());
            assertEquals(1L, search.totalHits.value());
            assertEquals(1, search.scoreDocs[0].doc);
        }
        {
            Query join = new JoinIndexQuery(fromSearcher, new TermQuery(new Term("id", "12")),
                    "fk", "id", indexManager,
                    indexWriterSupplier);
            TopDocs search = toSearcher.search(join, toSearcher.getIndexReader().maxDoc());
            assertEquals(1L, search.totalHits.value());
            assertEquals(0, search.scoreDocs[0].doc);
        }
        indexManager.close();
        toSearcher.getIndexReader().close();
        fromSearcher.getIndexReader().close();
        dir.close();
        fromDir.close();
        joinindexdir.close();
    }
}
