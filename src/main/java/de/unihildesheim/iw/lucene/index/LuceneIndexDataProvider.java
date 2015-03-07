/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.Tuple.Tuple3;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.AbstractIndexDataProviderBuilder.Feature;
import de.unihildesheim.iw.lucene.index.CollectionMetrics.CollectionMetricsConfiguration;
import de.unihildesheim.iw.lucene.search.EmptyFieldFilter;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
@Deprecated
public abstract class LuceneIndexDataProvider
    implements IndexDataProvider {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(LuceneIndexDataProvider.class);
  /**
   * Context for high precision math calculations.
   */
  static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
  /**
   * Collection metrics instance for this DataProvider.
   */
  private final CollectionMetrics metrics;

  /**
   * Information about the provided Lucene index.
   */
  private static final class LuceneIndex {
    /**
     * {@link IndexReader} to access the Lucene index.
     */
    private final IndexReader reader;
    /**
     * Number of all documents in lucene index.
     */
    private final int maxDocs;
    /**
     * Active document fields.
     */
    private final List<String> fields;
    /**
     * Frequency of all terms in index (respects active fields).
     */
    @Nullable
    private Long ttf;
    /**
     * Number of unique terms in index (respects active fields).
     */
    @Nullable
    private Long uniqueTerms;
    /**
     * List of stopwords. Initially empty.
     */
    private final BytesRefHash stopwords = new BytesRefHash();
    /**
     * Number of documents visible (documents having the required fields).
     */
    @Nullable
    private Integer docCount;
    /**
     * List of document-id of visible documents.
     */
    private Collection<Integer> docIds = Collections.emptyList();

    /**
     * Initialize the access to the Lucene index and gather basic information
     * about the index.
     *
     * @param r Reader to access the index
     * @param fieldSet List of fields to work with
     */
    LuceneIndex(final IndexReader r, final Collection<String> fieldSet) {
      final int fieldCount = fieldSet.size();
      if (fieldCount == 1) {
        this.fields = Collections.singletonList(
            fieldSet.toArray(new String[1])[0]);
      } else {
        this.fields = new ArrayList<>(fieldCount);
        this.fields.addAll(fieldSet);
      }

      this.reader = r;
      this.maxDocs = this.reader.maxDoc();
    }

    /**
     * Check if a term is flagged as stopword.
     *
     * @param br Term
     * @return True, if stopword
     */
    boolean isStopword(final BytesRef br) {
      return this.stopwords.find(br) != -1;
    }

    /**
     * Set the list of words to exclude
     *
     * @param sWords Stopwords list
     */
    void setStopwords(final Collection<String> sWords) {
      LOG.debug("Adding {} stopwords", sWords.size());
      sWords.forEach(w -> this.stopwords.add(new BytesRef(w)));
    }

    /**
     * Extend the list of words to exclude
     *
     * @param barry Stopwords list
     */
    void addStopwords(final BytesRefArray barry) {
      LOG.debug("Adding {} stopwords", barry.size());
      final BytesRefIterator barri = barry.iterator();

      try {
        for (BytesRef t = barri.next();
             t != null; t = barri.next()) {
          this.stopwords.add(t);
        }
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * Set the total term frequency value.
     *
     * @param newTtf New value
     */
    void setTtf(final long newTtf) {
      this.ttf = newTtf;
    }
  }

  /**
   * Object wrapping Lucene index information.
   */
  private final LuceneIndex index;
  /**
   * Feature configuration.
   */
  private final Map<Feature, Object> options = new EnumMap<>(Feature.class);

  /**
   * Create instance by using {@link Builder}.
   *
   * @param builder Builder
   * @throws DataProviderException If multiple fields are requested
   */
  private LuceneIndexDataProvider(final Builder builder)
      throws DataProviderException {
    if (builder.documentFields.size() > 1) {
      throw new DataProviderException(
          "Multiple fields support not implemented yet.");
    }

    // parse options
    for (final Entry<Feature, String> f :
        builder.supportedFeatures.entrySet()) {
      if (f.getValue() != null) {
        switch (f.getKey()) {
          case COMMON_TERM_THRESHOLD:
            LOG.info("CommonTerms threshold {}",
                Double.parseDouble(f.getValue()));
            this.options.put(f.getKey(), Double.parseDouble(f.getValue()));
            break;
        }
      }
    }

    // first initialize the Lucene index
    assert builder.idxReader != null;
    this.index = new LuceneIndex(builder.idxReader, builder.documentFields);
    // set initial list of stopwords passed in by builder
    this.index.setStopwords(builder.stopwords);

    LOG.info("Initializing index & gathering base data..");

    // get the number of documents before calculating the term frequency
    // values to allow removal of common terms (based on document frequency
    // values)
    LOG.debug("Estimating index size");

    try {
      this.index.docIds = getDocumentIdsCollection();
    } catch (final IOException e) {
      throw new DataProviderException("Failed to collect documents.", e);
    }
    this.index.docCount = this.index.docIds.size();

    // calculate total term frequency value for all current fields and get
    // the number of unique terms also
    // this will also collect stopwords, if a common-terms
    // threshold is set and a term exceeds this value
    LOG.info("Collecting term counts");
    final Tuple3<Long, Long, BytesRefArray> termCounts =
        collectTermFrequencyValues();
    // set gathered values
    this.index.setTtf(termCounts.a);
    this.index.uniqueTerms = termCounts.b;
    this.index.addStopwords(termCounts.c);

    // all data gathered, initialize metrics instance
    this.metrics = new CollectionMetrics(this,
        new CollectionMetricsConfiguration()
            .noCacheTf());

    LOG.debug("index.TTF {} index.UT {}", this.index.ttf,
        this.index.uniqueTerms);
    LOG.debug("TTF (abwasserreinigungsstuf): {}", getTermFrequency(new
        BytesRef("abwasserreinigungsstuf")));
  }

  /**
   * Check, if specific documents have TermVectors set for al  required fields.
   *
   * @param docIds Documents to check
   */
  private void checkForTermVectors(final Iterable<Integer> docIds) {
    boolean hasTermVectors = true;

    for (final int docId : docIds) {
      if (!hasDocument(docId)) {
        continue;
      }
      try {
        final Fields fields = this.index.reader.getTermVectors(docId);
        if (fields == null) {
          hasTermVectors = false;
          break;
        }
        for (final String field : this.index.fields) {
          if (fields.terms(field) == null) {
            hasTermVectors = false;
            break;
          }
        }
        if (!hasTermVectors) {
          // do not check further if we failed already
          break;
        }
      } catch (final IOException e) {
        // fail silently - maybe we succeed using no term vectors
        hasTermVectors = false;
        break;
      }
    }
    if (!hasTermVectors) {
      LOG.error("Document ids: {}", docIds);
      throw new IllegalStateException("TermVectors missing.");
    }
  }

  /**
   * Get the total term frequency of all terms in the index (respecting fields).
   * May also count the number of unique terms, while gathering frequency
   * values. If a common-terms threshold is set for the DataProvider terms will
   * be added to the list of stopwords if the threshold is exceeded.
   *
   * @return Tuple containing the total term frequency value (Tuple.A), the
   * number of unique terms (Tuple.B) and a list of terms exceeding the
   * common-terms threshold (if set) (Tuple.C).
   * @throws DataProviderException Thrown on low-level I/O errors
   */
  abstract Tuple3<Long, Long, BytesRefArray> collectTermFrequencyValues()
      throws DataProviderException;

  @Override
  public final long getTermFrequency() {
    // throws NPE, if not set on initialization time (intended)
    assert this.index.ttf != null;
    return this.index.ttf;
  }

  /**
   * Get a collection of all documents (their ids) in the index.
   *
   * @return Collection of found documents (their ids)
   */
  private Collection<Integer> getDocumentIdsCollection()
      throws IOException {
    LOG.debug("@getDocumentIdsCollection");
    // TODO: maybe back by MapDB
    final Collection<Integer> docIds =
        new ArrayList<>(this.index.reader.numDocs());
    LOG.info("Collecting all documents from index with field(s) {}",
        this.index.fields);

    final IndexSearcher searcher = new IndexSearcher(this.index.reader);
    TopDocs matches;

    final Query q = new MatchAllDocsQuery();

    matches = searcher.search(q, this.index.reader.numDocs());
    LOG.debug("MatchAllQuery returned {} matching documents.", matches
        .totalHits);

    for (final String field : this.index.fields) {
      @SuppressWarnings("ObjectAllocationInLoop")
      // require documents to have content in the current field
      final Filter fieldFilter = new EmptyFieldFilter(field);
      LOG.debug("Using FieldFilter: {}", fieldFilter);

      LOG.debug("Expecting {} documents at max.", this.index.reader.numDocs());
      matches = searcher.search(q, fieldFilter, this.index.reader.numDocs());
      LOG.debug("Query returned {} matching documents.", matches
          .totalHits);
      for (final ScoreDoc doc : matches.scoreDocs) {
        docIds.add(doc.doc);
      }
    }
    return docIds;
  }

  @Override
  public final IntStream getDocumentIds() {
    return this.index.docIds.stream()
        .mapToInt(i -> i);
  }

  @Override
  public final boolean hasDocument(final int docId) {
    return getDocumentIds()
        .filter(id -> id == docId)
        .findFirst()
        .isPresent();
  }

  @Override
  public Stream<BytesRef> getDocumentsTerms(final DocIdSet docIds) {
    try {
      final int[] docIdList = new int[DocIdSetUtils.cardinality(docIds)];
      final DocIdSetIterator disi = docIds.iterator();
      int idx = 0;
      for (int docId = disi.nextDoc();
           docId != DocIdSetIterator.NO_MORE_DOCS;
           docId = disi.nextDoc()) {
        docIdList[idx++] = docId;
      }

      TermsEnum termsEnum = TermsEnum.EMPTY;
      final BytesRefHash terms = new BytesRefHash();
      for (final String field : this.index.fields) {
        for (final Integer docId : docIdList) {
          final Terms docTerms = this.index.reader.getTermVector(docId, field);
          termsEnum = docTerms.iterator(termsEnum);
          BytesRef term = termsEnum.next();
          while (term != null) {
            terms.add(term);
            term = termsEnum.next();
          }
        }
      }
      return BytesRefUtils.streamBytesRefHash(terms);
    } catch (final IOException e) {
      throw new IllegalStateException("Error accessing Lucene index.", e);
    }
  }

  @Override
  public long getDocumentCount() {
    // FIXME: may be incorrect if there are more documents than max integer
    // value
    // throws NPE, if not set on initialization time (intended)
    assert this.index.docCount != null;
    return (long) this.index.docCount;
  }

  @Override
  public String[] getDocumentFields() {
    return this.index.fields.toArray(new String[this.index.fields.size()]);
  }

  @Override
  public BytesRefHash getStopwords() {
    return this.index.stopwords;
  }

  @Override
  public CollectionMetrics metrics() {
    return this.metrics;
  }

  /**
   * {@link LuceneIndexDataProvider} instance with methods optimized for usage
   * with a single document field only.
   */
  private static final class SingleFieldInstance
      extends LuceneIndexDataProvider {
    /**
     * Create instance by using {@link Builder}.
     *
     * @param builder Builder
     * @throws DataProviderException If multiple fields are requested
     */
    private SingleFieldInstance(
        final Builder builder)
        throws DataProviderException {
      super(builder);
      LOG.debug("LIDP-Type: SingleFieldInstance");
    }

    @Override
    public DocumentModel getDocumentModel(final int docId) {
      super.checkForTermVectors(Collections.singleton(docId));
      try {
        return new DocumentModel.Builder(docId)
            .setTermFrequency(
                getDocumentTerms(docId, false)
            ).getModel();
      } catch (final IOException e) {
        throw new IllegalStateException("Error accessing Lucene index.", e);
      }
    }

    @Override
    public int getDocumentFrequency(final BytesRef term) {
      try {
        final Terms terms = MultiFields.getTerms(
            super.index.reader, super.index.fields.get(0));
        final TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
        if (termsEnum.seekExact(term)) {
          return termsEnum.docFreq();
        }
      } catch (final IOException e) {
        throw new IllegalStateException("Error accessing Lucene index.", e);
      }
      return 0;
    }

    // WARN: Local class may not yet initialized when this method is called!
    // Use .THIS with caution!
    @Override
    public long getTermFrequency(final BytesRef term) {
      try {
        final Terms terms = MultiFields.getTerms(
            super.index.reader, super.index.fields.get(0));
        final TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
        return termsEnum.seekExact(term) ? termsEnum.totalTermFreq() : 0L;
      } catch (final IOException e) {
        throw new IllegalStateException("Error accessing Lucene index.", e);
      }
    }

    /**
     * Get a mapping (or list) of all terms in a specific document.
     *
     * @param docId Document id
     * @param asSet If true, only a set of terms will be created. If false terms
     * are mapped to their (within document) term frequency value.
     * @return List of terms or mapping of term to (within document) term
     * frequency value
     * @throws IOException Thrown on low-level I/O errors
     */
    private Map<BytesRef, Long> getDocumentTerms(
        final int docId, final boolean asSet)
        throws IOException {
      final Map<BytesRef, Long> termsMap = new HashMap<>(500);
      final Terms terms = super.index.reader.getTermVector(
          docId, super.index.fields.get(0));
      if (terms == null) {
        LOG.warn("No Term Vectors for field {} in document {}.",
            super.index.fields.get(0));
        LOG.debug("Field exists? {}", super.index.reader.document(docId)
            .getField(super.index.fields.get(0)) != null);
      } else {
        final TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
        BytesRef term = termsEnum.next();

        while (term != null) {
          if (!super.index.isStopword(term)) {
            if (asSet) {
              termsMap.put(BytesRef.deepCopyOf(term), null);
            } else {
              if (termsMap.containsKey(term)) {
                termsMap.put(BytesRef.deepCopyOf(term),
                    termsMap.get(term) + termsEnum.totalTermFreq());
              } else {
                termsMap.put(BytesRef.deepCopyOf(term),
                    termsEnum.totalTermFreq());
              }
            }
          }
          term = termsEnum.next();
        }
      }
      return termsMap;
    }

    // WARN: Local class may not yet initialized when this method is called!
    // Use .THIS with caution!
    @Override
    Tuple3<Long, Long, BytesRefArray> collectTermFrequencyValues()
        throws DataProviderException {
      final double ctThreshold;
      final BytesRefArray newStopwords;
      final boolean skipCommonTerms = super.options.containsKey(
          Feature.COMMON_TERM_THRESHOLD);

      newStopwords = new BytesRefArray(Counter.newCounter(false));
      if (skipCommonTerms) {
        ctThreshold = (double) super.options.get(Feature.COMMON_TERM_THRESHOLD);
      } else {
        ctThreshold = 1d;
      }

      TermsEnum termsEnum = TermsEnum.EMPTY;
      @Nullable BytesRef currentTerm; // current iteration term
      long uniqueCount = 0L; // number of unique terms
      long ttf = 0L; // final total term frequency value

      try {
        @Nullable
        final Terms terms = MultiFields.getTerms(
            super.index.reader, super.index.fields.get(0));
        termsEnum = terms.iterator(termsEnum);

        assert super.index.docCount != null;
        currentTerm = termsEnum.next();
        while (currentTerm != null) {
          if (!super.index.isStopword(currentTerm)) {
            if (skipCommonTerms && ((double) termsEnum.docFreq() /
                (double) super.index.docCount) > ctThreshold) {
              newStopwords.append(currentTerm);
            } else {
              ttf += termsEnum.totalTermFreq();
              uniqueCount++;
            }
          }
          currentTerm = termsEnum.next();
        }
        return new Tuple3<>(ttf, uniqueCount, newStopwords);
      } catch (final IOException e) {
        throw new DataProviderException("Failed to collect terms for field '" +
            super.index.fields.get(0) + "'", e);
      }
    }
  }

  /**
   * {@link LuceneIndexDataProvider} instance with methods optimized for usage
   * with multiple document fields.
   */
  private static final class MultiFieldInstance
      extends LuceneIndexDataProvider {

    /**
     * Create instance by using {@link Builder}.
     *
     * @param builder Builder
     * @throws DataProviderException If multiple fields are requested
     */
    private MultiFieldInstance(
        final Builder builder)
        throws DataProviderException {
      super(builder);
      LOG.debug("LIDP-Type: MultiFieldInstance");
    }

    @Override
    public int getDocumentFrequency(final BytesRef term) {
      // TODO: implement
      throw new UnsupportedOperationException();
    }

    @Override
    public DocumentModel getDocumentModel(final int docId) {
      // TODO: implement
      throw new UnsupportedOperationException();
    }

    @Override
    public long getTermFrequency(final BytesRef term) {
      // TODO: implement
      throw new UnsupportedOperationException();
    }

    @Override
    Tuple3<Long, Long, BytesRefArray> collectTermFrequencyValues()
        throws DataProviderException {
      final double ctThreshold;
      final BytesRefHash newStopwords;
      final boolean skipCommonTerms = super.options.containsKey(
          Feature.COMMON_TERM_THRESHOLD);

      newStopwords = new BytesRefHash();
      if (skipCommonTerms) {
        ctThreshold = (double) super.options.get(Feature.COMMON_TERM_THRESHOLD);
      } else {
        ctThreshold = 1d;
      }

      @Nullable Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      @Nullable DocsEnum docsEnum = null;

      // term -> tf
      final Map<BytesRef, Long> termMap = new HashMap<>(5000000);

      int doc;
      @Nullable BytesRef term;
      final Bits liveDocs = MultiFields.getLiveDocs(super.index.reader);
      // go through all fields
      for (final String field : super.index.fields) {
        LOG.debug("Collecting terms from field '{}'.", field);
        try {
          terms = MultiFields.getTerms(super.index.reader, field);
          if (terms == null) {
            LOG.warn("Field '{}' not found. Skipping.", field);
            continue;
          }

          termsEnum = terms.iterator(termsEnum);

          // iterate over all terms in field
          term = termsEnum.next();
          while (term != null) {
            if (!super.index.isStopword(term)) {
              // add term to stopwords list, if it exceeds the document
              // frequency threshold
              // TODO: may need a df check over all fields? - check all
              // together at the end?
              assert super.index.docCount != null;
              if (skipCommonTerms &&
                  (((double) termsEnum.docFreq() /
                      (double) super.index.docCount) >
                      ctThreshold)) {
                newStopwords.add(term);
              }

              // skip terms already flagged as stopword
              if (newStopwords.find(term) == -1) {
                // obtain frequency so far, if term was already seen
                Long termFreq = termMap.get(term);
                if (termFreq == null) {
                  termFreq = 0L;
                }

                // go through all documents which contain the current term
                docsEnum = termsEnum.docs(liveDocs, docsEnum);
                doc = docsEnum.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                  termFreq += (long) docsEnum.freq();
                  doc = docsEnum.nextDoc();
                }
                termMap.put(BytesRef.deepCopyOf(term), termFreq);
              }
            }
            term = termsEnum.next();
          }
        } catch (final IOException e) {
          throw new DataProviderException(
              "Failed to collect terms for field '" + field + "'", e);
        }
      }

      // TODO: check df for all remaining terms here ?

      return new Tuple3<>(
          // sum up all single tf values
          termMap.values().stream().reduce(0L, (x, y) -> x + y),
          // number of unique terms
          (long) termMap.keySet().size(),
          // new stopwords based on threshold
          BytesRefUtils.hashToArray(newStopwords));
    }
  }

  /**
   * Builder for creating a new {@link LuceneIndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractIndexDataProviderBuilder<Builder> {

    /**
     * Constructor setting the supported features.
     */
    public Builder() {
      super(new Feature[]{
          Feature.COMMON_TERM_THRESHOLD
      });
    }

    @Override
    Builder getThis() {
      return this;
    }

    @Override
    public LuceneIndexDataProvider build()
        throws BuildException, ConfigurationException {
      validate();
      assert this.idxReader != null;
      if (this.idxReader.hasDeletions()) {
        throw new BuildException(
            "Index with deletions is currently not supported.");
      }
      try {
        return this.documentFields.size() == 1 ?
            new SingleFieldInstance(this) :
            new MultiFieldInstance(this);
      } catch (final DataProviderException e) {
        throw new BuildException("Failed to build instance.", e);
      }
    }
  }
}
