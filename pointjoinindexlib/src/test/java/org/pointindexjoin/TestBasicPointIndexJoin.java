package org.pointindexjoin;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;

public class TestBasicPointIndexJoin extends LuceneTestCase {
    public void testBasic() throws IOException {
        Directory dir = newDirectory();
        Directory fromDir = newDirectory();
        Directory jonnindexdir = newDirectory();
        RandomIndexWriter w =
                new RandomIndexWriter(
                        random(),
                        dir,
                        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
        RandomIndexWriter fromw =
                new RandomIndexWriter(
                        random(),
                        dir,
                        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

        w.addDocument(doc);

        IndexSearcher indexSearcher = new IndexSearcher(w.getReader());
        IndexSearcher fromSearcher = new IndexSearcher(fromw.getReader());
        w.close();
        Query join = new JoinIndexQuery(jonnindexdir, fromSearcher, new TermQuery(new Term("sku_id","2")));
        TopDocs search = indexSearcher.search(join, indexSearcher.getIndexReader().maxDoc());

    }
}
