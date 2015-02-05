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

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.Tuple.Tuple3;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.AbstractIndexDataProviderBuilder.Feature;
import de.unihildesheim.iw.lucene.index.CollectionMetrics.CollectionMetricsConfiguration;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.ByteArrayUtils;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldValueFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class LuceneIndexDataProvider
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
    private Set<ByteArray> stopwords = Collections.emptySet();
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
    final boolean isStopword(final BytesRef br) {
      return this.stopwords.contains(BytesRefUtils.toByteArray(br));
    }

    /**
     * Check if a term is flagged as stopword.
     *
     * @param ba Term
     * @return True, if stopword
     */
    boolean isStopword(final ByteArray ba) {
      return this.stopwords.contains(ba);
    }

    /**
     * Set the list of words to exclude
     *
     * @param sWords Stopwords list
     */
    final void setStopwords(final Collection<String> sWords) {
      LOG.debug("Adding {} stopwords", sWords.size());
      this.stopwords = new HashSet<>(sWords.size());

      this.stopwords.addAll(sWords.stream()
          .map(s -> new ByteArray(s.getBytes(StandardCharsets.UTF_8)))
          .collect(Collectors.toList()));
    }

    /**
     * Extend the list of words to exclude
     *
     * @param baStopwords Stopwords list
     */
    final void addStopwords(final Collection<ByteArray> baStopwords) {
      LOG.debug("Adding {} stopwords", baStopwords.size());
      this.stopwords.addAll(baStopwords);
    }

    /**
     * Set the total term frequency value.
     *
     * @param newTtf New value
     */
    final void setTtf(final long newTtf) {
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
    this.index.docIds = getDocumentIdsCollection();
    this.index.docCount = this.index.docIds.size();

    // calculate total term frequency value for all current fields and get
    // the number of unique terms also
    // this will also collect stopwords, if a common-terms
    // threshold is set and a term exceeds this value
    LOG.info("Collecting term counts");
    final Tuple3<Long, Long, Set<ByteArray>> termCounts =
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
        ByteArray("abwasserreinigungsstuf"
        .getBytes(StandardCharsets.UTF_8))));
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
        } else {
          for (final String field : this.index.fields) {
            if (fields.terms(field) == null) {
              hasTermVectors = false;
              break;
            }
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
  private Tuple3<Long, Long, Set<ByteArray>> collectTermFrequencyValues()
      throws DataProviderException {
    final double ctThreshold;
    final Set<ByteArray> newStopwords;
    final boolean skipCommonTerms = this.options.containsKey(
        Feature.COMMON_TERM_THRESHOLD);

    if (skipCommonTerms) {
      newStopwords = new HashSet<>(1000);
      ctThreshold = (double) this.options.get(Feature.COMMON_TERM_THRESHOLD);
    } else {
      newStopwords = Collections.emptySet();
      ctThreshold = 1d;
    }

    @Nullable Terms terms;
    TermsEnum termsEnum = TermsEnum.EMPTY;
    @Nullable DocsEnum docsEnum = null;

    if (this.index.fields.size() > 1) {
      // term, tf
      final Map<ByteArray, Long> termMap = new HashMap<>(5000000);

      int doc;
      @Nullable BytesRef termBr;
      final Bits liveDocs = MultiFields.getLiveDocs(this.index.reader);
      // go through all fields
      for (final String field : this.index.fields) {
        LOG.debug("Collecting terms from field '{}'.", field);
        try {
          terms = MultiFields.getTerms(this.index.reader, field);
          if (terms == null) {
            LOG.warn("Field '{}' not found. Skipping.", field);
            continue;
          }

          termsEnum = terms.iterator(termsEnum);

          // iterate over all terms in field
          termBr = termsEnum.next();
          while (termBr != null) {
            if (!this.index.isStopword(termBr)) {
              final ByteArray termBa = BytesRefUtils.toByteArray(termBr);

              // add term to stopwords list, if it exceeds the document
              // frequency threshold
              assert this.index.docCount != null;
              if (skipCommonTerms &&
                  (((double) termsEnum.docFreq() /
                      (double) this.index.docCount) >
                      ctThreshold)) {
                newStopwords.add(termBa);
              }

              // skip terms already flagged as stopword
              if (!newStopwords.contains(termBa)) {
                // obtain frequency so far, if term was already seen
                Long termFreq = termMap.get(termBa);
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
                termMap.put(termBa, termFreq);
              }
            }
            termBr = termsEnum.next();
          }
        } catch (final IOException e) {
          throw new DataProviderException(
              "Failed to collect terms for field '" + field + "'", e);
        }
      }

      return new Tuple3<>(
          // sum up all single tf values
          termMap.values().stream().reduce(0L, (x, y) -> x + y),
          // number of unique terms
          (long) termMap.keySet().size(),
          // new stopwords based on threshold
          newStopwords);
    } else {
      @Nullable BytesRef currentTerm; // current iteration term
      long uniqueCount = 0L; // number of unique terms
      long ttf = 0L; // final total term frequency value

      try {
        terms = MultiFields.getTerms(
            this.index.reader, this.index.fields.get(0));
        termsEnum = terms.iterator(termsEnum);

        assert this.index.docCount != null;
        currentTerm = termsEnum.next();
        while (currentTerm != null) {
          if (!this.index.isStopword(currentTerm)) {
            if (((double) termsEnum.docFreq() / (double) this.index.docCount) >
                ctThreshold) {
              newStopwords.add(BytesRefUtils.toByteArray(currentTerm));
            } else {
              ttf += termsEnum.totalTermFreq();
              uniqueCount++;
            }
          }
          currentTerm = termsEnum.next();
        }
        return new Tuple3<>(ttf, uniqueCount, newStopwords);
      } catch (final IOException e) {
        throw new DataProviderException(
            "Failed to collect terms for field '" +
                this.index.fields.get(0) + "'", e);
      }
    }
  }

  @Override
  public final long getTermFrequency() {
    // throws NPE, if not set on initialization time (intended)
    assert this.index.ttf != null;
    return this.index.ttf;
  }

  @Override
  public final Long getTermFrequency(final ByteArray term) {
    // TODO: add multiple fields support
    try {
      final Terms terms = MultiFields.getTerms(
          this.index.reader, this.index.fields.get(0));
      final TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
      if (termsEnum.seekExact(BytesRefUtils.fromByteArray(term))) {
        return termsEnum.totalTermFreq();
      } else {
        return 0L;
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Error accessing Lucene index.", e);
    }
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    LOG.debug("@getDocumentFrequency");
    // TODO: add multiple fields support
    try {
      final Terms terms = MultiFields.getTerms(
          this.index.reader, this.index.fields.get(0));
      final TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
      if (termsEnum.seekExact(BytesRefUtils.fromByteArray(term))) {
        return termsEnum.docFreq();
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Error accessing Lucene index.", e);
    }

    return 0;
  }

  /**
   * Get a collection of all documents (their ids) in the index.
   *
   * @return Collection of found documents (their ids)
   */
  private Collection<Integer> getDocumentIdsCollection() {
    LOG.debug("@getDocumentIdsCollection");
    // TODO: maybe back by MapDB
    final Collection<Integer> docIds = new ArrayList<>(this.index.maxDocs);
    LOG.info("Collecting all documents from index with field(s) {}",
        this.index.fields);

    final IndexSearcher searcher = new IndexSearcher(this.index.reader);
    TopDocs matches;

    final Query q = new MatchAllDocsQuery();

    for (final String field : this.index.fields) {
      @SuppressWarnings("ObjectAllocationInLoop")
      // require documents to have content in the current field
      final Filter fieldFilter = new FieldValueFilter(field);
      LOG.debug("Using FieldFilter: {}", fieldFilter);

      try {
        matches = searcher.search(q, fieldFilter, this.index.reader.numDocs());
        LOG.debug("Query returned {} matching documents.", matches.totalHits);
        for (final ScoreDoc doc : matches.scoreDocs) {
          docIds.add(doc.doc);
        }
      } catch (final IOException e) {
        e.printStackTrace();
      }
    }
    return docIds;
  }

  @Override
  public final Stream<Integer> getDocumentIds() {
    return this.index.docIds.parallelStream();
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkForTermVectors(Collections.singleton(docId));
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
  public final boolean hasDocument(final int docId) {
    return getDocumentIds()
        .filter(id -> id == docId)
        .findFirst()
        .isPresent();
  }

  /**
   * Get a mapping (or list) of all terms in a specific document.
   * @param docId Document id
   * @param asSet If true, only a set of terms will be created. If false
   * terms are mapped to their (within document) term frequency value.
   * @return List of terms or mapping of term to (within document) term
   * frequency value
   * @throws IOException Thrown on low-level I/O errors
   */
  private Map<ByteArray, Long> getDocumentTerms(
      final int docId, final boolean asSet)
      throws IOException {
    final Map<ByteArray, Long> termsMap = new HashMap<>(500);

    // TODO: add support for multiple fields
    final Terms terms = this.index.reader.getTermVector(
        docId, this.index.fields.get(0));
    if (terms == null) {
      LOG.warn("No Term Vectors for field {} in document {}.", this.index
          .fields.get(0), docId);
      LOG.debug("Field exists? {}", (this.index.reader.document(docId)
          .getField(this.index.fields.get(0)) != null));
    } else {
      final TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
      BytesRef term = termsEnum.next();

      while (term != null) {
        if (!this.index.isStopword(term)) {
          if (asSet) {
            termsMap.put(BytesRefUtils.toByteArray(term), null);
          } else {
            final ByteArray termBytes = BytesRefUtils.toByteArray(term);
            if (termsMap.containsKey(termBytes)) {
              termsMap.put(termBytes, termsMap.get(termBytes)
                  + termsEnum.totalTermFreq());
            } else {
              termsMap.put(termBytes, termsEnum.totalTermFreq());
            }
          }
        }
        term = termsEnum.next();
      }
    }
    return termsMap;
  }

  @Override
  public Stream<ByteArray> getDocumentsTerms(
      final Set<Integer> docIds) {
    TermsEnum termsEnum = TermsEnum.EMPTY;
    final Collection<ByteArray> termSet = new HashSet<>(docIds.size() * 100);
    try {
      for (final String field : this.index.fields) {
        for (final Integer docId : docIds) {
          final Terms terms = this.index.reader.getTermVector(docId, field);
          termsEnum = terms.iterator(termsEnum);
          BytesRef term = termsEnum.next();
          while (term != null) {
            termSet.add(BytesRefUtils.toByteArray(term));
            term = termsEnum.next();
          }
        }
      }
      return termSet.stream();
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
  public Set<String> getDocumentFields() {
    return new HashSet<>(this.index.fields);
  }

  @Override
  public Set<String> getStopwords() {
    final Set<String> words = new HashSet<>(this.index.stopwords.size());
    words.addAll(this.index.stopwords.stream().map(ByteArrayUtils::utf8ToString)
        .collect(Collectors.toList()));
    return words;
  }

  @Override
  public CollectionMetrics metrics() {
    return this.metrics;
  }

//  private static final class DocIdBits
//      implements Bits {
//    final int l;
//    final Collection<Integer> docs;
//
//    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
//    DocIdBits(final Collection<Integer> docIds) {
//      this.l = docIds.size();
//      this.docs = docIds;
//    }
//
//    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
//    DocIdBits(final int length, final Collection<Integer> docIds) {
//      this.l = length;
//      this.docs = docIds;
//    }
//
//    @Override
//    public final boolean get(final int i) {
//      return this.docs.contains(i);
//    }
//
//    @Override
//    public final int length() {
//      return this.l;
//    }
//  }

//  /**
//   * Iterator over all terms from a list of documents.
//   */
//  private final class DocTermsIterator
//      implements Iterator<ByteArray> {
//    /**
//     * Current term.
//     */
//    private BytesRef nextTerm;
//    /**
//     * Terms enumerator for the given field.
//     */
//    private final TermsEnum te;
//    /**
//     * Bits turned on for the specified documents only.
//     */
//    private final Bits bits;
//    /**
//     * Documents enumerator for current term.
//     */
//    private final DocsEnum de = null;
//
//    /**
//     * Default constructor with field and document ids. All terms from the
//     * specified field will be collected for the given list of documents only.
//     *
//     * @param field Field to collect terms from
//     * @param docIds Documents whose terms should be collected
//     * @throws IOException Thrown on low-level I/O errors
//     */
//    DocTermsIterator(final String field, final Collection<Integer> docIds)
//        throws IOException {
//      this.te = MultiFields.getTerms(
//          LuceneIndexDataProvider.this.index.reader, field).iterator(
//          TermsEnum.EMPTY);
//      this.bits = new DocIdBits(docIds);
//      this.nextTerm = this.te.next();
//    }
//
//    @Override
//    public final boolean hasNext() {
//      return this.nextTerm != null;
//    }
//
//    @Override
//    public final ByteArray next() {
//      final ByteArray term;
//      try {
//        term = BytesRefUtils.toByteArray(this.nextTerm);
//        // get the next term, skipping those, not in our target documents
//        // by checking a docsEnum for contents
//        do {
//          this.nextTerm = this.te.next();
//        } while (this.nextTerm != null &&
//            this.te.docs(this.bits, this.de).nextDoc() ==
//                DocIdSetIterator.NO_MORE_DOCS);
//      } catch (final IOException e) {
//        throw new IllegalStateException("Error getting next term.", e);
//      }
//      return term;
//    }
//  }

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
      try {
        return new LuceneIndexDataProvider(this);
      } catch (final DataProviderException e) {
        throw new BuildException("Failed to build instance.", e);
      }
    }
  }
}
