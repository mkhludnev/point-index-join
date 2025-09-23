package org.pointindexjoin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
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
import org.apache.lucene.util.BytesRef;

public class JoinAssertion implements LongSupplier {

    private static final Logger LOGGER = Logger.getLogger(JoinAssertion.class.getName());

    // Inputs
    protected final List<String> selectedChildIds;
    protected final IndexSearcher fromSearcher;
    private final Map<String, String> childToParentMap;
    private final SearcherManager indexManager;
    private final Supplier<IndexWriter> indexWriterSupplier;
    private final IndexSearcher toSearcher;
    private final Random random;
    private JoinIndexQuery joinIndexQuery;

    // Internal state
    private Set<String> parentsExpected;
    private String removedParent;
    private long wallClock = 0L;

    // Constructor
    public JoinAssertion(List<String> selectedChildIds,
                         Map<String, String> childToParentMap,
                         IndexSearcher fromSearcher,
                         SearcherManager indexManager,
                         Supplier<IndexWriter> indexWriterSupplier,
                         IndexSearcher toSearcher, Random random) {
        this.selectedChildIds = Objects.requireNonNull(selectedChildIds, "selectedChildIds must not be null");
        this.childToParentMap = Objects.requireNonNull(childToParentMap, "childToParentMap must not be null");
        this.fromSearcher = Objects.requireNonNull(fromSearcher, "fromSearcher must not be null");
        this.indexManager = Objects.requireNonNull(indexManager, "indexManager must not be null");
        this.indexWriterSupplier = Objects.requireNonNull(indexWriterSupplier, "indexWriterSupplier must not be null");
        this.toSearcher = Objects.requireNonNull(toSearcher, "toSearcher must not be null");
        this.random = random;
    }

    @Override
    public long getAsLong() {
        return wallClock;
    }

    // Main execution method
    public JoinAssertion assertJoin() throws Exception {
        logChildren();
        computeExpectedParents();
        maybeRemoveRandomParent();
        Query joinQuery = buildJoinQuery();
        long start = System.currentTimeMillis();
        TopDocs search = toSearcher.search(joinQuery, selectedChildIds.size());
        wallClock += System.currentTimeMillis() - start;
        validateSearchResults(search);
        verifyAllParentsFound(search);
        if (joinIndexQuery != null) {
            joinIndexQuery.close();
        }
        return this;
    }

    private void logChildren() {
        LOGGER.info("children: " + selectedChildIds);
    }

    private void computeExpectedParents() {
        parentsExpected = selectedChildIds.stream()
                .map(childToParentMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        LOGGER.info("parents expected: " + parentsExpected);
    }

    private void maybeRemoveRandomParent() {
        if (!parentsExpected.isEmpty() && random.nextBoolean()) {
            Iterator<String> iterator = parentsExpected.iterator();
            removedParent = iterator.next();
            iterator.remove();
            LOGGER.info("removed parent: " + removedParent + ". Parents expected: " + parentsExpected);
        }
    }

    private Query buildJoinQuery() throws IOException {
        long start = System.currentTimeMillis();
        Query baseQuery = createCoreJoinQuery();
        wallClock += System.currentTimeMillis() - start;

        if (removedParent != null) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(baseQuery, BooleanClause.Occur.MUST);
            builder.add(new TermQuery(new Term("id", removedParent)), BooleanClause.Occur.MUST_NOT);
            builder.add(new TermInSetQuery("id", parentsExpected.stream().map(BytesRef::new).toList()),
                    BooleanClause.Occur.FILTER);
            baseQuery = builder.build();
        }
        return baseQuery;
    }

    protected Query createCoreJoinQuery() throws IOException {
        joinIndexQuery = new JoinIndexQuery(
                fromSearcher,
                new TermInSetQuery("id", selectedChildIds.stream().map(BytesRef::new).toList()),
                "fk", "id",
                indexManager,
                indexWriterSupplier
        );

        return joinIndexQuery;
    }

    private void validateSearchResults(TopDocs search) {
        Set<String> actualParents = Arrays.stream(search.scoreDocs, 0, (int) search.totalHits.value())
                .map(sd -> {
                    try {
                        return toSearcher.storedFields().document(sd.doc).get("id");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toSet());

        String message = String.format(
                "%s should join to %s but actually joined to %s",
                selectedChildIds,
                parentsExpected,
                String.join(",", actualParents)
        );

        if (parentsExpected.size() != search.totalHits.value()) {
            throw new AssertionError(message);
        }
    }

    private void verifyAllParentsFound(TopDocs search) throws IOException {
        for (ScoreDoc doc : search.scoreDocs) {
            Document document = toSearcher.storedFields().document(doc.doc);
            String parentId = document.get("id");
            if (!parentsExpected.remove(parentId)) {
                throw new AssertionError("Unexpected parent found: " + parentId);
            }
        }
        if (!parentsExpected.isEmpty()) {
            throw new AssertionError("Not all expected parents were found: " + parentsExpected);
        }
    }
}