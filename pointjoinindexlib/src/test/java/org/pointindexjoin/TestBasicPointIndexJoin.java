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

        {
            Document child1 = new Document();
            child1.add(new SortedSetDocValuesField("fk",new BytesRef("1")));
            child1.add(new StringField("id","11", Field.Store.YES));
            fromw.addDocument(child1);
            child1 = new Document();
            child1.add(new SortedSetDocValuesField("fk",new BytesRef("1")));
            child1.add(new StringField("id","12", Field.Store.YES));
            fromw.addDocument(child1);
        }
        {
            Document child2 = new Document();
            child2.add(new SortedSetDocValuesField("fk", new BytesRef("2")));
            child2.add(new StringField("id", "21", Field.Store.YES));
            fromw.addDocument(child2);
            child2 = new Document();
            child2.add(new SortedSetDocValuesField("fk", new BytesRef("2")));
            child2.add(new StringField("id", "22", Field.Store.YES));
            fromw.addDocument(child2);
        }
        {
            Document parent1= new Document();
            parent1.add(new SortedSetDocValuesField("id", new BytesRef("1")));
            w.addDocument(parent1);
        }
        {
            Document parent2= new Document();
            parent2.add(new SortedSetDocValuesField("id", new BytesRef("2")));
            w.addDocument(parent2);
        }
        w.commit();
        fromw.commit();


        IndexSearcher indexSearcher = new IndexSearcher(w.getReader());
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
        {
            Query join = new JoinIndexQuery(fromSearcher, new TermQuery(new Term("id", "22")),
                    "fk", "id", new SearcherManager(joinindexdir, null),
                    indexWriterSupplier);
            TopDocs search = indexSearcher.search(join, indexSearcher.getIndexReader().maxDoc());
            assertEquals(1L, search.totalHits.value());
            assertEquals(1, search.scoreDocs[0].doc);
        }
        {
            Query join = new JoinIndexQuery(fromSearcher, new TermQuery(new Term("id", "12")),
                    "fk", "id", new SearcherManager(joinindexdir, null),
                    indexWriterSupplier);
            TopDocs search = indexSearcher.search(join, indexSearcher.getIndexReader().maxDoc());
            assertEquals(1L, search.totalHits.value());
            assertEquals(0, search.scoreDocs[0].doc);
        }
    }
}
