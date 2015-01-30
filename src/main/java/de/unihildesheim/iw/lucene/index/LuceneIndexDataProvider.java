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
import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.AbstractIndexDataProviderBuilder
    .Feature;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.ByteArrayUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class LuceneIndexDataProvider
    implements IndexDataProvider {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(LuceneIndexDataProvider.class);
  /**
   * Prefix used to store {@link GlobalConfiguration configuration} data.
   */
  private static final String IDENTIFIER = "LuceneIDP";
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
  private static class LuceneIndex {
    /**
     * {@link IndexReader} to access the Lucene index.
     */
    private final IndexReader reader;
    /**
     * Cache livedocs value from current IndexReader.
     */
    private final Bits liveDocs;
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
    private Set<ByteArray> stopwords = Collections.EMPTY_SET;
    /**
     * Number of documents visible (documents having the required fields).
     */
    @Nullable
    private Integer docCount;
    /**
     * List of document-id of visible documents.
     */
    private Collection<Integer> docIds = Collections.EMPTY_LIST;

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
      this.liveDocs = MultiFields.getLiveDocs(this.reader);
      this.maxDocs = this.reader.maxDoc();
    }

    /**
     * Check, if we have a total term frequency value set.
     *
     * @return True, if value is available
     */
    boolean hasTtf() {
      return this.ttf != null;
    }

    /**
     * Check if document count is set.
     * @return True, if set
     */
    boolean hasDocCount() {
      return this.docCount != null;
    }

    /**
     * Check if a term is flagged as stopword.
     * @param br Term
     * @return True, if stopword
     */
    boolean isStopword(final BytesRef br) {
      return this.stopwords.contains(BytesRefUtils.toByteArray(br));
    }

    /**
     * Check if a term is flagged as stopword.
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
    @SuppressWarnings("ObjectAllocationInLoop")
    void setStopwords(final Collection<String> sWords) {
      LOG.debug("Adding {} stopwords", sWords.size());
      this.stopwords = new HashSet<>(sWords.size());

      for (final String s : sWords) {
        this.stopwords.add(new ByteArray(s.getBytes(StandardCharsets.UTF_8)));
      }
    }

    /**
     * Extend the list of words to exclude
     *
     * @param brStopwords Stopwords list
     */
    void addStopwords(final Collection<BytesRef> brStopwords) {
      LOG.debug("Adding {} stopwords", brStopwords.size());
      for (final BytesRef br : brStopwords) {
        this.stopwords.add(BytesRefUtils.toByteArray(br));
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
  private final Map<Feature, Object> options = new EnumMap(Feature.class);

  /**
   * Create instance by using {@link Builder}.
   * @param builder Builder
   * @throws DataProviderException If multiple fields are requested
   */
  public LuceneIndexDataProvider(final Builder builder)
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
    LOG.debug("Collecting term counts");
    final Tuple3<Long, Long, Set<BytesRef>> termCounts =
        collectTermFrequencyValues();
    // set gathered values
    this.index.setTtf(termCounts.a);
    this.index.uniqueTerms = termCounts.b;
    this.index.addStopwords(termCounts.c);

    // all data gathered, initialize metrics instance
    this.metrics = new CollectionMetrics(this);

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
   * @throws DataProviderException Thrown on low-level I/O errors
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
   * values.
   * If a common-terms threshold is set for the DataProvider terms will be
   * added to the list of stopwords if the threshold is exceeded.
   *
   * @return Tuple containing the TTf value (Tuple.A), the
   * number of unique terms (Tuple.B) and a list of terms exceeding the
   * common-terms threshold (if set) (Tuple.C).
   * @throws DataProviderException Thrown on low-level I/O errors
   */
  private Tuple3<Long, Long, Set<BytesRef>> collectTermFrequencyValues()
      throws DataProviderException {
    long uniqueCount = 0L;
    final boolean skipCommonTerms = this.options.containsKey(
        Feature.COMMON_TERM_THRESHOLD);
    double ctThreshold = 1d;
    final Set<BytesRef> newStopwords = new HashSet<>(1000);
    // TODO: add multiple fields support
    try {
      final LuceneTermsIteratorRetrieveFlags[] flags;
      if (skipCommonTerms) {
        LOG.debug("Collecting common terms");
        ctThreshold = (double) this.options.get(Feature.COMMON_TERM_THRESHOLD);
        flags = new LuceneTermsIteratorRetrieveFlags[]{
            LuceneTermsIteratorRetrieveFlags.TTF,
            LuceneTermsIteratorRetrieveFlags.DF};
      } else {
        flags = new LuceneTermsIteratorRetrieveFlags[]{
            LuceneTermsIteratorRetrieveFlags.TTF};
      }
      final LuceneTermsIterator ttfIt = new LuceneTermsIterator(
          this.index.fields.get(0), flags);

      Long ttf = 0L; // final total term frequency value
      while (ttfIt.hasNext()) {
        if (skipCommonTerms) {
          // save term to add to stopwords list
          final BytesRef term = ttfIt.next();
          // throws NPE, if docCount not set on initialization time (intended)
          if (((double) ttfIt.df() / (double) this.index.docCount) >
          ctThreshold) {
            newStopwords.add(term);
            continue; // next term, skip this one
          }
        } else {
          ttfIt.next(); // term is not of interest
        }
        ttf += ttfIt.ttf();
        uniqueCount++;
      }
      return new Tuple3<>(ttf, uniqueCount, newStopwords);
    } catch (final IOException e) {
      throw new DataProviderException("Failed to collect terms", e);
    }
  }

  @Override
  public final long getTermFrequency() {
    // throws NPE, if not set on initialization time (intended)
    return this.index.ttf;
  }

  @Override
  public final Long getTermFrequency(final ByteArray term) {
    // short circuit for stopwords
    if (this.index.isStopword(term)) {
      return 0L;
    }

    // TODO: add multiple fields support
    try {
      final Terms terms = MultiFields.getTerms(
          this.index.reader, this.index.fields.get(0));
      final TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
      if (termsEnum.seekExact(new BytesRef(term.bytes))) {
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

  @Override // NOP
  public void close() {
    // NOP
  }

  /**
   * Get a collection of all documents (their ids) in the index.
   * @return Collection of found documents (their ids)
   */
  private Collection<Integer> getDocumentIdsCollection() {
    // TODO: maybe back by MapDB
    final Collection<Integer> docIds = new ArrayList(this.index.maxDocs);
    LOG.info("Collecting all documents from index with field(s) {}",
        this.index.fields);

    final IndexSearcher searcher = new IndexSearcher(this.index.reader);
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
        CharArraySet.EMPTY_SET);
    Query query;
    final TotalHitCountCollector totalHitsCollector =
        new TotalHitCountCollector();
    TopDocs matches;

    for (final String field : this.index.fields) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final QueryParser qp = new QueryParser(
          LuceneDefaults.VERSION, field, analyzer);
      qp.setAllowLeadingWildcard(true);
      try {
        query = qp.parse("*");
        searcher.search(query, totalHitsCollector);
        final int expResults = totalHitsCollector.getTotalHits();
        LOG.debug("Running query expecting {} results.", expResults);
        matches = searcher.search(query, expResults);
        LOG.debug("Query returned {} matching documents.", matches.totalHits);
        for (final ScoreDoc doc : matches.scoreDocs) {
          docIds.add(doc.doc);
        }
      } catch (ParseException | IOException e) {
        e.printStackTrace();
      }
    }
    return docIds;
  }

  @Override
  public Iterator<Integer> getDocumentIds() {
    return this.index.docIds.iterator();
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
  public boolean hasDocument(final int docId) {
    final Iterator<Integer> docIdIt = getDocumentIds();
    while (docIdIt.hasNext()) {
      if (docId == docIdIt.next()) {
        return true;
      }
    }
    return false;
  }

  private Map<ByteArray, Long> getDocumentTerms(
      final int docId, final boolean asSet)
      throws IOException {
    final Map<ByteArray, Long> termsMap = new HashMap<>();

    // TODO: add support for multiple fields
    final Terms terms = this.index.reader.getTermVector(
        docId, this.index.fields.get(0));
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
    return termsMap;
  }

  private final class LuceneDocTermsSpliterator
  extends AbstractSpliterator<ByteArray> {
    /**
     * {@link TermsEnum} pointing at the target field.
     */
    private final TermsEnum termsEnum;
    /**
     * Next term in the list of provided terms.
     */
    private BytesRef nextTerm;
    /**
     * Indicate, if there are any stopwords to use.
     */
    private final boolean useStopwords;
    /**
     * Document ids to gather terms from.
     */
    private final List<Integer> docIds;
    /**
     * Documents with current term enumerator.
     */
    private DocsEnum docsEnum;

    private LuceneDocTermsSpliterator(
        final String field, final Collection<Integer> documentIds)
        throws IOException {
      super(Long.MAX_VALUE,
          Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE);

      final Terms terms = MultiFields.getTerms(LuceneIndexDataProvider
          .this.index.reader, field);
      this.termsEnum = terms.iterator(TermsEnum.EMPTY);
      this.useStopwords = !LuceneIndexDataProvider.this
          .index.stopwords.isEmpty();

      this.docIds = new ArrayList<>(documentIds);
      Collections.sort(this.docIds);

      setNextTerm();
    }

    private void getNextTerm()
        throws IOException {
      if (this.useStopwords) {
        do {
          this.nextTerm = this.termsEnum.next();
        } while (this.nextTerm != null &&
            LuceneIndexDataProvider.this.index.isStopword(this.nextTerm));
      } else {
        this.nextTerm = this.termsEnum.next();
      }
    }

    private void setNextTerm()
        throws IOException {
      getNextTerm();
      boolean haveNext = false;
      while (this.nextTerm != null && !haveNext) {
        this.docsEnum = this.termsEnum.docs(
            LuceneIndexDataProvider.this.index.liveDocs,
            this.docsEnum, DocsEnum.FLAG_NONE);

        for (final int docId : this.docIds) {
          final int doc = this.docsEnum.advance(docId);
          if (doc == DocsEnum.NO_MORE_DOCS) {
            break;
          }
          if (doc == docId || this.docIds.contains(doc)) {
            haveNext = true;
            break;
          }
        }
        if (!haveNext) {
          getNextTerm();
        }
      }
    }

    @Override
    public boolean tryAdvance(final Consumer<? super ByteArray> action) {
      if (action == null) {
        throw new NullPointerException();
      }

      if (this.nextTerm == null) {
        return false;
      }

      action.accept(BytesRefUtils.toByteArray(this.nextTerm));

      try {
        setNextTerm();
      } catch (final IOException e) {
        LOG.error("Failed retrieving next term.", e);
        return false;
      }

      return true;
    }
  }

  @Override
  public Stream<ByteArray> getDocumentsTerms(
      final Collection<Integer> docIds) {
    // TODO: add support for multiple fields
    try {
      return StreamSupport.stream(new LuceneDocTermsSpliterator(
          this.index.fields.get(0), docIds), true);
    } catch (final IOException e) {
      throw new IllegalStateException("Error accessing Lucene index.", e);
    }
  }

  @Override
  public long getDocumentCount() {
    // FIXME: may be incorrect if there are more documents than max integer
    // value
    // throws NPE, if not set on initialization time (intended)
    return (long) this.index.docCount;
  }

  @Override
  public Set<String> getDocumentFields() {
    return new HashSet<>(this.index.fields);
  }

  @Override
  public Set<String> getStopwords() {
    final Set<String> words = new HashSet<>(this.index.stopwords.size());
    for (final ByteArray ba : this.index.stopwords) {
      words.add(ByteArrayUtils.utf8ToString(ba));
    }
    return words;
  }

  @Override
  public CollectionMetrics metrics() {
    return this.metrics;
  }

  /**
   * Abstract implementation to iterate over terms in a Lucene document field.
   *
   * @param <A> Type of value returned by the {@link Iterator}
   */
  private abstract class AbstractLuceneTermsIterator<A>
      implements Iterator<A> {
    /**
     * {@link Terms} instance for target field.
     */
    protected final Terms terms;
    /**
     * {@link TermsEnum} pointing at the target field.
     */
    protected TermsEnum termsEnum;
    /**
     * Next term in the list of provided terms.
     */
    protected BytesRef nextTerm;
    /**
     * Indicate, if there are any stopwords to use.
     */
    private final boolean useStopwords;

    /**
     * Default constructor setting up the required {@link TermsEnum}.
     *
     * @param field Field to query for terms
     * @throws IOException Thrown on low-level I/O errors
     */
    AbstractLuceneTermsIterator(final String field)
        throws IOException {
      this.terms = MultiFields.getTerms(LuceneIndexDataProvider
          .this.index.reader, field);
      this.termsEnum = TermsEnum.EMPTY;
      this.termsEnum = this.terms.iterator(this.termsEnum);
      this.useStopwords = !LuceneIndexDataProvider.this
          .index.stopwords.isEmpty();
    }

    /**
     * Forwards to the next term in the {@link TermsEnum}.
     *
     * @throws IOException Thrown on low-level I/O errors
     */
    protected void setNext()
        throws IOException {
      if (this.useStopwords) {
        do {
          this.nextTerm = this.termsEnum.next();
        } while (this.nextTerm != null &&
            LuceneIndexDataProvider.this.index.isStopword(this.nextTerm));
      } else {
        this.nextTerm = this.termsEnum.next();
      }
    }

    @Override
    public boolean hasNext() {
      return this.nextTerm != null;
    }
  }

  /**
   * Simple terms iterator converting {@link BytesRef} objects to {@link
   * ByteArray} on the fly.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  final class LuceneByteTermsIterator
      extends AbstractLuceneTermsIterator<ByteArray> {
    /**
     * Current term.
     */
    private BytesRef term;

    /**
     * Default constructor setting up the required {@link TermsEnum}.
     *
     * @param field Field to query for terms
     * @throws IOException Thrown on low-level I/O errors
     */
    LuceneByteTermsIterator(final String field)
        throws IOException {
      super(field);
    }

    @Override
    public ByteArray next() {
      this.term = this.nextTerm;
      if (this.term == null) {
        throw new NoSuchElementException();
      }
      try {
        setNext();
      } catch (final IOException e) {
        LOG.error("Failed to get next term.", e);
      }
      return BytesRefUtils.toByteArray(this.term);
    }
  }

  /**
   * Iterator to access Lucene document terms by field.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  final class LuceneDocTermsIterator
      extends AbstractLuceneTermsIterator<ByteArray> {
    /**
     * Current term.
     */
    private ByteArray term;
    /**
     * Document ids to gather terms from.
     */
    private final List<Integer> docIds;
    /**
     * Documents with current term enumerator.
     */
    private DocsEnum docsEnum;

    /**
     * Iterate over all terms from a list of documents.
     * @param field Documents field
     * @param documentIds Documents to extract terms from
     * @throws IOException Thrown on low-level I/O errors
     */
    public LuceneDocTermsIterator(final String field, final
    Collection<Integer> documentIds)
        throws IOException {
      super(field);
      this.docIds = new ArrayList<>(documentIds);
      Collections.sort(this.docIds);
      setNext();
    }

    /**
     * Get the next element in order.
     * @throws IOException Thrown on low-level I/O errors
     */
    @Override
    protected void setNext()
        throws IOException {
      super.setNext();
      boolean haveNext = false;
      while (this.nextTerm != null && !haveNext) {
          this.docsEnum = this.termsEnum.docs(
              LuceneIndexDataProvider.this.index.liveDocs,
              this.docsEnum, DocsEnum.FLAG_NONE);

          for (final int docId : this.docIds) {
            final int doc = this.docsEnum.advance(docId);
            if (doc == DocsEnum.NO_MORE_DOCS) {
              break;
            }
            if (doc == docId || this.docIds.contains(doc)) {
              // store term in cache
              haveNext = true;
              break;
            }
          }
          if (!haveNext) {
            super.setNext();
          }
      }
    }

    @Override
    public ByteArray next() {
      if (this.nextTerm == null) {
        throw new NoSuchElementException();
      }
      this.term = BytesRefUtils.toByteArray(this.nextTerm);
      try {
        setNext();
      } catch (final IOException e) {
        LOG.error("Failed to get next term.", e);
      }
      return this.term;
    }
  }

  /**
   * Additional data retrieval flags.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  enum LuceneTermsIteratorRetrieveFlags {
    /**
     * Retrieve total term frequency values.
     */
    TTF(0),
    /**
     * Retrieve document frequency values.
     */
    DF(1);

    /**
     * Index to {@link LuceneTermsIterator#retrieveFlags} array.
     */
    private final int idx;

    /**
     * Initialize a value setting the index to {@link
     * LuceneTermsIterator#retrieveFlags}
     * array.
     *
     * @param arrIdx Array index position
     */
    private LuceneTermsIteratorRetrieveFlags(final int arrIdx) {
      this.idx = arrIdx;
    }
  }

  /**
   * Iterator to access Lucene document terms by field.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  final class LuceneTermsIterator
      extends AbstractLuceneTermsIterator<BytesRef> {
    /**
     * Current term.
     */
    private BytesRef term;

    /**
     * List of possible retrieval flags.
     */
    private final boolean[] retrieveFlags =
        new boolean[LuceneTermsIteratorRetrieveFlags.values().length];
    /**
     * Total term frequency value for current term.
     */
    private long ttf = -1L;
    /**
     * Document frequency value for current term.
     */
    private int df = -1;

    /**
     * Iterate through all terms of a given field. Optionally retrieving
     * additional information for each term encountered.
     *
     * @param field Field to get terms from
     * @param flags Set optional values that should be retrieved for each term
     * @throws IOException Thrown on low-level I/O errors
     */
    private LuceneTermsIterator(final String field,
        @Nullable final LuceneTermsIteratorRetrieveFlags[] flags)
        throws IOException {
      super(field);

      Arrays.fill(this.retrieveFlags, false);

      if (flags != null && flags.length > 0) {
        for (final LuceneTermsIteratorRetrieveFlags flag : flags) {
          this.retrieveFlags[flag.idx] = true;
        }
      }

      setNext();
    }

    /**
     * Retrieves the total term frequency value for the current term.
     *
     * @return TTF value for the current term or {@code -1L}, if not enabled
     */
    public long ttf() {
      return this.ttf;
    }

    /**
     * Retrieves the total term frequency value for the current term.
     *
     * @return TTF value for the current term or {@code -1L}, if not enabled
     */
    public int df() {
      return this.df;
    }

    @Override
    public BytesRef next() {
      if (this.nextTerm == null) {
        throw new NoSuchElementException();
      }
      // copy bytes, since reference may get out of scope
      this.term = new BytesRef(BytesRefUtils.copyBytes(this.nextTerm));
      try {
        // retrieve values for current term..
        if (this.retrieveFlags[LuceneTermsIteratorRetrieveFlags.TTF.idx]) {
          this.ttf = this.termsEnum.totalTermFreq();
        }
        if (this.retrieveFlags[LuceneTermsIteratorRetrieveFlags.DF.idx]) {
          this.df = this.termsEnum.docFreq();
        }
        // ..before advancing to the next term
        setNext();
      } catch (final IOException e) {
        LOG.error("Failed to get next term.", e);
      }
      return this.term;
    }
  }

  /**
   * Builder for creating a new {@link LuceneIndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractIndexDataProviderBuilder<Builder> {

    /**
     * Features supported by this {@link IndexDataProvider}.
     */
    private static final Feature[] FEATURES = {
        Feature.COMMON_TERM_THRESHOLD
    };

    /**
     * Constructor setting the implementation identifier for the cache.
     */
    public Builder() {
      setSupportedFeatures(FEATURES);
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
