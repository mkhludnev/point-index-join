package org.pointindexjoin;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestDVIntersect extends LuceneTestCase {

    public void testBasic() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir,
                new IndexWriterConfig()
                //                .setInfoStream(new JavaLoggingInfoStream(Level.INFO))
        );
        Set<String> fks = new HashSet<>();
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < atLeast(100); i++) {
            Document parent1 = new Document();
            String id = Integer.toString(random().nextInt(100));
            ids.add(id);
            parent1.add(new SortedSetDocValuesField("id", new BytesRef(id)));
            String fkid = Integer.toString(random().nextInt(100));
            parent1.add(new SortedSetDocValuesField("fk", new BytesRef(fkid)));
            fks.add(fkid);
            w.addDocument(parent1);
        }
        w.commit();
        DirectoryReader reader = DirectoryReader.open(dir);

        Set<String> intesect = new HashSet<>(ids);
        intesect.retainAll(fks);
        int intersectSize = intesect.size();
        SortedSetDocValues fkDV = reader.leaves().getFirst().reader().getSortedSetDocValues("fk");
        SortedSetDocValues idDV = reader.leaves().getFirst().reader().getSortedSetDocValues("id");
        int[] fkOrdByIdOrd = JoinIndexHelper.innerJoinTerms(fkDV,
                idDV);
        assertEquals(fkDV.getValueCount(), fks.size());
        assertEquals(fkOrdByIdOrd.length, ids.size());
        assertEquals(idDV.getValueCount(), ids.size());
        int found = 0, notfound = 0;
        for (int idOrd = 0; idOrd < fkOrdByIdOrd.length; idOrd++) {
            String idVal = idDV.lookupOrd(idOrd).utf8ToString();
            if (intesect.remove(idVal)) {
                int fkOrd = fkOrdByIdOrd[idOrd];
                String fkVal = fkDV.lookupOrd(fkOrd).utf8ToString();
                assertEquals(idVal, fkVal);
                found++;
            } else {
                notfound++;
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, fkOrdByIdOrd[idOrd]);
            }
        }
        assertEquals(intersectSize, found);
        assertEquals(ids.size(), found + notfound);
        reader.close();
        reader.directory().close();
    }
}
