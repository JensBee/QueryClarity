/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
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
import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.mapdb.DBMakerUtils;
import de.unihildesheim.iw.util.BigDecimalCache;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * @author Jens Bertram
 */
public class DirectAccessIndexDataProvider
    implements IndexDataProvider {
  /**
   * Context for high precision math calculations.
   */
  static final MathContext MATH_CONTEXT = MathContext.DECIMAL128;
  /**
   * Static value to use map as set.
   */
  private static final Boolean IGNORED_MAP_VALUE = Boolean.TRUE;
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(DirectAccessIndexDataProvider.class);
  /**
   * Prefix used to store {@link GlobalConfiguration configuration} data.
   */
  private static final String IDENTIFIER = "DirectAccessIDP";
  /**
   * {@link IndexReader} to access the Lucene index.
   */
  private final IndexReader idxReader;
  /**
   * Document fields.
   */
  private final Set<String> fields;
  /**
   * Stopwords in byte representation.
   */
  private final Set<ByteArray> stopwords;

  /**
   * Disk-backed storage for large collections.
   */
  private final DB cache;
  /**
   * Stopwords as string.
   */
  private final Set<String> stopwordsStr;
  /**
   * Cached index frequency values for single terms.
   */
  private final BTreeMap<ByteArray, Long> termFreqCache;
  /**
   * Cached document frequency values for single terms.
   */
  private final BTreeMap<ByteArray, Integer> docFreqCache;
  /**
   * Cached list of document ids that have content in the current field(s) and
   * are not marked as deleted.
   */
  private final BTreeMap<Integer, Object> documentIds;
  /**
   * Cached list of all terms currently available in all known documents and
   * fields.
   */
  private final BTreeMap<ByteArray, Object> idxTerms;
  /**
   * Stores last commit generation of the Lucene index.
   */
  private final Long idxCommitGeneration;
  /**
   * Keep track of temporary databases to close on closing this instance.
   */
  private final WeakHashMap<DB, Object> tempCaches;
  /**
   * Cache for calculated term sets from multiple documents.
   */
  private final DB termSetCacheDB;
  /**
   * Cache keys for calculated term sets from multiple documents.
   */
  private final List<List<Integer>> termSetCacheEntries;
  /**
   * Cache livedocs value from current IndexReader.
   */
  private final Bits liveDocs;
  /**
   * Cached leaves of the current IndexReader.
   */
  private final List<AtomicReaderContext> idxReaderLeaves;
  /**
   * Frequency of all known terms in the index.
   */
  private long idxTermFrequency = -1L;
  /**
   * Flag indicating, if this instance has been closed.
   */
  private boolean isClosed;

  /**
   * Builder based constructor.
   *
   * @param builder Builder to use for constructing the instance
   * @throws DataProviderException Wrapped IOException thrown if initializing
   * caches fails on low-level I/O errors.
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public DirectAccessIndexDataProvider(final Builder builder)
      throws DataProviderException {
    // set stopwords
    this.stopwordsStr = new HashSet<>(builder.stopwords.size());
    this.stopwordsStr.addAll(builder.stopwords);
    this.stopwords = new HashSet<>(builder.stopwords.size());
    for (final String word : this.stopwordsStr) {
      this.stopwords.add(new ByteArray(word.getBytes(StandardCharsets.UTF_8)));
    }

    this.idxReader = builder.idxReader;
    this.idxCommitGeneration = builder.lastCommitGeneration;
    this.fields = new HashSet<>(builder.documentFields.size());
    this.fields.addAll(builder.documentFields);

    // create cache data structures
    this.tempCaches = new WeakHashMap<>(100);
    this.cache = DBMakerUtils.newTempFileDB().make();
    this.termFreqCache = this.cache.createTreeMap("termFreqCache")
        .keySerializer(ByteArray.SERIALIZER_BTREE)
        .valueSerializer(Serializer.LONG)
        .make();
    this.docFreqCache = this.cache.createTreeMap("docFreqCache")
        .keySerializer(ByteArray.SERIALIZER_BTREE)
        .valueSerializer(Serializer.INTEGER)
        .make();
    this.documentIds = this.cache.createTreeMap("documentIds")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
            //.valueSerializer(Serializer.BOOLEAN)
        .valueSerializer(null)
        .counterEnable()
        .make();
    this.idxTerms = this.cache.createTreeMap("idxTerms")
        .keySerializer(ByteArray.SERIALIZER_BTREE)
            //.valueSerializer(Serializer.BOOLEAN)
        .valueSerializer(null)
        .counterEnable()
        .make();

    this.termSetCacheDB = DBMakerUtils.newCompressedTempFileDB().make();
    this.tempCaches.put(this.termSetCacheDB, Boolean.TRUE);
    this.termSetCacheEntries = new ArrayList<>(1000);

    // cache some Lucene values
    this.liveDocs = MultiFields.getLiveDocs(this.idxReader);
    this.idxReaderLeaves = this.idxReader.getContext().leaves();

    try {
      initCache();
    } catch (final IOException | ParseException e) {
      throw new DataProviderException("Failed to initialize caches.", e);
    }
  }

  /**
   * Initialized values that have to be cached beforehand.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws DataProviderException Wrapped IOException Thrown on low-level I/O
   * errors
   */
  private void initCache()
      throws IOException, DataProviderException, ParseException {
    collectDocuments();
    collectIndexTerms();
  }

  /**
   * Collect ids of documents that have at least one of the current fields set.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException Thrown if the query estimating the maximum of
   * matching documents has failed
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  private void collectDocuments()
      throws IOException, ParseException {
    LOG.info("Collecting all documents from index with field(s) {}",
        this.fields);
    final TimeMeasure tm = new TimeMeasure().start();

    final IndexSearcher searcher = new IndexSearcher(this.idxReader);
    QueryParser qp;
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
        CharArraySet.EMPTY_SET);
    Query query;
    final TotalHitCountCollector totalHitsCollector =
        new TotalHitCountCollector();
    TopDocs matches;

    for (final String field : this.fields) {
      qp = new QueryParser(LuceneDefaults.VERSION, field, analyzer);
      qp.setAllowLeadingWildcard(true);
      query = qp.parse("*");

      searcher.search(query, totalHitsCollector);
      final int expResults = totalHitsCollector.getTotalHits();
      LOG.debug("Running query expecting {} results.", expResults);
      matches = searcher.search(query, expResults);
      LOG.debug("Query returned {} matching documents.", matches.totalHits);
      for (final ScoreDoc doc : matches.scoreDocs) {
        this.documentIds.put(doc.doc, IGNORED_MAP_VALUE);
      }
    }

    LOG.info("Collecting {} documents from index with field(s) {} took {}",
        this.documentIds.size(), this.fields, tm.stop().getTimeString());
  }

  /**
   * Collects all terms from all currently set fields.
   *
   * @throws DataProviderException Wrapped {@link IOException} which gets thrown
   * on low-level I/O errors
   */
  private void collectIndexTerms()
      throws DataProviderException {
    if (this.stopwords.isEmpty()) {
      LOG.info("Collecting all terms from index (fields {})", this.fields);
    } else {
      LOG.info(
          "Collecting all terms from index (fields {}), excluding stopwords",
          this.fields);
    }
    final TimeMeasure tm = new TimeMeasure().start();

    Terms terms;
    TermsEnum termsEnum = TermsEnum.EMPTY;

    // iterate over all fields
    for (final String field : this.fields) {
      try {
        terms = getMultiTermsInstance(field);
        if (terms != null) {
          termsEnum = terms.iterator(termsEnum);
          BytesRef term = termsEnum.next();
          while (term != null) {
            final ByteArray termBytes = BytesRefUtils.toByteArray(term);
            // skip, if stopword
            if (!this.stopwords.contains(termBytes)) {
              if (termsEnum.seekExact(term)) {
                this.idxTerms.put(termBytes, IGNORED_MAP_VALUE);
              }
            }
            term = termsEnum.next();
          }
        }
      } catch (final IOException e) {
        throw new DataProviderException(
            "Caught exception trying to get terms from field '"
                + field + "'.", e);
      }
    }

    LOG.info("Collecting {} terms from index (fields {}) took {}",
        this.idxTerms.size(), this.fields, tm.stop().getTimeString());
  }

  /**
   * Get a {@link MultiTerms} instance for the given field combining all {@link
   * AtomicReaderContext leaves} of the current {@link IndexReader}.
   *
   * @param field Field to get the terms from
   * @return {@link MultiTerms} instance or null, if there are no leaves or no
   * document with the given field
   * @throws IOException Thrown on low-level I/O errors
   */
  @SuppressWarnings({"ReturnOfNull", "ObjectAllocationInLoop"})
  private Terms getMultiTermsInstance(final String field)
      throws IOException {
    switch (this.idxReaderLeaves.size()) {
      case 0:
        return null;
      case 1:
        return this.idxReaderLeaves.get(0).reader().terms(field);
      default:
        final List<ReaderSlice> slices =
            new ArrayList<>(this.idxReaderLeaves.size());
        final List<Terms> terms = new ArrayList<>(this.idxReaderLeaves.size());

        for (final AtomicReaderContext context : this.idxReaderLeaves) {
          final AtomicReader aReader = context.reader();
          final Terms rTerms = aReader.terms(field);
          if (rTerms != null) {
            terms.add(rTerms);
            slices.add(new ReaderSlice(
                context.docBase, aReader.maxDoc(),
                aReader.fields().size() - 1));
          }
        }

        if (terms.isEmpty()) {
          return null;
        } else if (terms.size() == 1) {
          return terms.get(0);
        } else {
          return new MultiTerms(terms.toArray(Terms.EMPTY_ARRAY),
              slices.toArray(ReaderSlice.EMPTY_ARRAY));
        }
    }
  }

  @Override
  public long getTermFrequency()
      throws DataProviderException {
    if (this.idxTermFrequency < 0L) {
      // already calculated
      return this.idxTermFrequency;
    }

    LOG.info("Calculating index term frequency value for fields {}",
        this.fields);
    final TimeMeasure tm = new TimeMeasure().start();

    long freq = 0L; // final frequency value
    Terms terms;
    TermsEnum termsEnum = TermsEnum.EMPTY;

    // iterate over all fields
    for (final String field : this.fields) {
      try {
        terms = getMultiTermsInstance(field);

        if (terms == null) {
          // field does not exist
          continue;
        }

        if (this.stopwords.isEmpty()) {
          // no stopwords set - simply add all term frequencies
          freq += terms.getSumTotalTermFreq();
        } else {
          // stopwords are set - we have to check each term manually
          termsEnum = terms.iterator(termsEnum);
          BytesRef term = termsEnum.next();

          // iterate over all terms in current field
          while (term != null) {
            if (!this.stopwords.contains(BytesRefUtils.toByteArray(term))) {
              // docsEnum should never get null, field and term are already
              // checked
              final DocsEnum docsEnum = getTermDocsEnum(field, term);
              int docId = docsEnum.nextDoc();

              // iterate over all documents that have the current term in the
              // current field
              while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                freq += (long) docsEnum.freq();
                docId = docsEnum.nextDoc();
              }
            }
            term = termsEnum.next();
          }
        }
      } catch (final IOException e) {
        throw new DataProviderException(
            "Caught exception trying to get terms from field '"
                + field + "'.", e);
      }
    }

    LOG.info("Calculating index term frequency value for fields {} took {}",
        this.fields, tm.stop().getTimeString());

    this.idxTermFrequency = freq;
    return freq;
  }

  @Override
  public void warmUp()
      throws DataProviderException {
    // do nothing here
  }

  @Override
  public Long getTermFrequency(final ByteArray term)
      throws DataProviderException {
    if (this.stopwords.contains(term)) {
      // stopword - term not available
      return 0L;
    }

    // check for a cached value
    Long freq = this.termFreqCache.get(term);
    if (freq == null) {
      freq = 0L;
      // calculate value
      Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      final BytesRef termBytes = BytesRefUtils.refFromByteArray(term);

      // iterate over all fields
      for (final String field : this.fields) {
        try {
          terms = getMultiTermsInstance(field);
          if (terms != null) {
            termsEnum = terms.iterator(termsEnum);
            if (termsEnum.seekExact(termBytes)) {
              // docsEnum should never get null, field and term are already
              // checked
              final DocsEnum docsEnum = getTermDocsEnum(field, termBytes);
              int docId = docsEnum.nextDoc();

              // iterate over all documents that have the current term in the
              // current field
              while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                freq += (long) docsEnum.freq();
                docId = docsEnum.nextDoc();
              }
            }
          }
        } catch (final IOException e) {
          throw new DataProviderException(
              "Caught exception trying to get terms from field '"
                  + field + "'.", e);
        }
      }

      this.termFreqCache.put(term, freq);
    }
    return freq;
  }

  @Override
  public int getDocumentFrequency(final ByteArray term)
      throws DataProviderException {
    if (this.stopwords.contains(term)) {
      // stopword - term not available
      return 0;
    }

    // check for a cached value
    Integer freq = this.docFreqCache.get(term);
    if (freq == null) {
      // calculate value
      Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      final BytesRef termBytes = BytesRefUtils.refFromByteArray(term);
      final Set<Integer> collectedDocIds = new HashSet<>(500);

      // iterate over all fields
      DocsEnum docsEnum;
      for (final String field : this.fields) {
        try {
          terms = getMultiTermsInstance(field);
          if (terms != null) {
            termsEnum = terms.iterator(termsEnum);
            if (termsEnum.seekExact(termBytes)) {
              // docsEnum should never get null, field and term are already
              // checked
              docsEnum = getTermDocsEnum(field, termBytes);
              int docId = docsEnum.nextDoc();

              // iterate over all documents that have the current term in the
              // current field
              while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                collectedDocIds.add(docId);
                docId = docsEnum.nextDoc();
              }
            }
          }
        } catch (final IOException e) {
          throw new DataProviderException(
              "Caught exception trying to get terms from field '"
                  + field + "'.", e);
        }
      }
      freq = collectedDocIds.size();
      this.docFreqCache.put(term, freq);
    }
    return freq;
  }

  /**
   * Get a {@link DocsEnum} for a specific field and term. This merges all
   * {@link AtomicReaderContext leaves} of the current {@link IndexReader}.
   *
   * @param field Field to match
   * @param term Term to match
   * @return DocsEnm instance or null, if field does not exist
   * @throws IOException Thrown on low-level I/O errors
   */
  @SuppressWarnings("ReturnOfNull")
  private DocsEnum getTermDocsEnum(final String field, final BytesRef term)
      throws IOException {
    final Terms terms = getMultiTermsInstance(field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator(null);
      if (termsEnum.seekExact(term)) {
        return termsEnum.docs(this.liveDocs, null, DocsEnum.FLAG_FREQS);
      }
    }
    return null;
  }

  @Override
  public BigDecimal getRelativeTermFrequency(final ByteArray term)
      throws DataProviderException {
    final long tf = getTermFrequency(term);
    if (tf == 0L) {
      return BigDecimal.ZERO;
    }
    return BigDecimalCache.get(tf).divide(
        BigDecimalCache.get(getTermFrequency()), MATH_CONTEXT);
  }

  @Override
  public void close() {
    LOG.info("Closing IndexDataProvider.");
    if (!this.tempCaches.isEmpty()) {
      LOG.debug("Releasing temporary files.");
      for (final DB db : this.tempCaches.keySet()) {
        if (!db.isClosed()) {
          db.close();
        }
      }
    }
    if (!this.cache.isClosed()) {
      LOG.debug("Releasing cache.");
      this.cache.close();
    }
    this.isClosed = true;
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    return this.idxTerms.keySet().iterator();
  }

  @Override
  public Iterator<Integer> getDocumentIds() {
    return this.documentIds.keySet().iterator();
  }

  @Override
  public long getUniqueTermsCount() {
    return this.idxTerms.sizeLong();
  }

  @SuppressWarnings("ReturnOfNull")
  @Override
  public DocumentModel getDocumentModel(final int docId)
      throws DataProviderException {
    if (!hasDocument(docId)) {
      return null;
    }
    return new DocumentModel.Builder(docId)
        .setTermFrequency(
            getDocumentsTermsMap(Collections.singleton(docId))
        ).getModel();
  }

  @Override
  public boolean hasDocument(final int docId) {
    return this.documentIds.containsKey(docId);
  }

  @Override
  public Map<ByteArray, Long> getDocumentTerms(final int docId)
      throws DataProviderException {
    if (!hasDocument(docId)) {
      return Collections.emptyMap();
    }
    return getDocumentsTermsMap(Collections.singleton(docId));
  }

  @Override
  public Iterator<Map.Entry<ByteArray, Long>> getDocumentsTerms(
      final Collection<Integer> docIds)
      throws DataProviderException {
    return getDocumentsTermsMap(docIds).entrySet().iterator();
  }

  @Override
  public Set<ByteArray> getDocumentTermsSet(final int docId)
      throws DataProviderException {
    if (!hasDocument(docId)) {
      return Collections.emptySet();
    }
    return getDocumentsTermsMap(Collections.singleton(docId)).keySet();
  }

  @Override
  public Iterator<ByteArray> getDocumentsTermsSet(
      final Collection<Integer> docIds)
      throws DataProviderException {
    return getDocumentsTermsMap(docIds).keySet().iterator();
  }

  @Override
  public long getDocumentCount()
      throws DataProviderException {
    return this.documentIds.sizeLong();
  }

  @Override
  public boolean documentContains(final int documentId, final ByteArray term)
      throws DataProviderException {
    if (!hasDocument(documentId)) {
      return false;
    }
    final BytesRef termBytes = BytesRefUtils.refFromByteArray(term);
    DocsEnum docsEnum;

    // iterate over all fields
    for (final String field : this.fields) {
      try {
        docsEnum = getTermDocsEnum(field, termBytes);
        // iterate over all documents and the current field containing the
        // given term
        if (docsEnum != null) {
          int docId = docsEnum.nextDoc();
          while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            if (docId == documentId) {
              // document found
              return true;
            }
            docId = docsEnum.nextDoc();
          }
        }
      } catch (final IOException e) {
        throw new DataProviderException(
            "Caught exception trying to get documents for field '"
                + field + "'.", e);
      }
    }
    return false;
  }

  @Override
  public Long getLastIndexCommitGeneration() {
    return this.idxCommitGeneration;
  }

  @Override
  public Set<String> getDocumentFields() {
    return Collections.unmodifiableSet(this.fields);
  }

  @Override
  public Set<String> getStopwords() {
    return Collections.unmodifiableSet(this.stopwordsStr);
  }

  @Override
  public Set<ByteArray> getStopwordsBytes() {
    return Collections.unmodifiableSet(this.stopwords);
  }

  @Override
  public boolean isClosed() {
    return this.isClosed;
  }

  /**
   * Get a mapping of term->frequency for a set of documents
   *
   * @param docIds Document id's to extract the terms from
   * @return Mapping of term->frequency for all specified documents
   * @throws DataProviderException Wrapped {@link IOException} which gets thrown
   * on low-level I/O errors
   */
  private Map<ByteArray, Long> getDocumentsTermsMap(
      final Collection<Integer> docIds)
      throws DataProviderException {
    final Map<ByteArray, Long> termsMap = getCachedTermSetMap(docIds);

    // already cached
    if (!termsMap.isEmpty()) {
      return termsMap;
    }

    boolean hasTermVectors = true;
    // see, if we can use termVectors - it's usually faster
    for (final int docId : docIds) {
      if (!hasDocument(docId)) {
        continue;
      }
      try {
        final Fields fields = this.idxReader.getTermVectors(docId);
        if (fields == null) {
          hasTermVectors = false;
          break;
        } else {
          for (final String field : this.fields) {
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

    if (hasTermVectors) {
//      LOG.debug("Collecting terms using TermVectors.");
      try {
        final DocFieldsTermsEnum dftEnum =
            new DocFieldsTermsEnum(this.idxReader, this.fields);
        BytesRef term;
        // step through all fields of each document
        for (final int docId : docIds) {
          dftEnum.setDocument(docId);
          term = dftEnum.next();

          // collect terms from document fields
          while (term != null) {
            final ByteArray termBytes = BytesRefUtils.toByteArray(term);
            // skip, if stopword
            if (!this.stopwords.contains(termBytes)) {
              if (termsMap.containsKey(termBytes)) {
                termsMap.put(termBytes, termsMap.get(termBytes)
                    + dftEnum.getTotalTermFreq());
              } else {
                termsMap.put(termBytes, dftEnum.getTotalTermFreq());
              }
            }
            term = dftEnum.next();
          }
        }
      } catch (final IOException e) {
        // fail silently - maybe we succeed using no term vectors
        hasTermVectors = false;
        // we may have already produced results, so delete them
        termsMap.clear();
      }
    }

    // re-check again, if termVectors are there, but collecting terms has failed
    if (!hasTermVectors) {
//      LOG.debug("Collecting terms using direct method.");
      Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      BytesRef term;
      DocsEnum docsEnum;
      for (final String field : this.fields) {
        try {
          terms = getMultiTermsInstance(field);
          if (terms != null) {
            termsEnum = terms.iterator(termsEnum);
            term = termsEnum.next();
            while (term != null) {
              if (termsEnum.seekExact(term)) {
                docsEnum = getTermDocsEnum(field, term);
                final int docId = docsEnum.nextDoc();
                while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                  if (docIds.contains(docId)) {
                    final ByteArray termBytes = BytesRefUtils.toByteArray(term);
                    if (termsMap.containsKey(termBytes)) {
                      termsMap.put(termBytes, termsMap.get(termBytes)
                          + (long) docsEnum.freq());
                    } else {
                      termsMap.put(termBytes, (long) docsEnum.freq());
                    }
                  }
                }
              }
            }
          }
        } catch (final IOException e) {
          throw new DataProviderException(
              "Caught exception trying to get terms from field '"
                  + field + "'.", e);
        }
      }
    }

    return termsMap;
  }

  /**
   * Tries to load an already calculated TermSetMap. If none was cached an new
   * empty map will be created.
   *
   * @param docIds Document ids for which to build the map
   * @return The cached map or an empty new one
   */
  private Map<ByteArray, Long> getCachedTermSetMap(
      final Collection<Integer> docIds) {
    final List<Integer> docIdsSorted = new ArrayList<>(docIds);

    synchronized (this.termSetCacheEntries) {
      Integer mapId = this.termSetCacheEntries.indexOf(docIdsSorted);
      if (mapId < 0) {
        mapId = this.termSetCacheEntries.size();
        this.termSetCacheEntries.add(docIdsSorted);
      }

      // if there's already a map with the combination of document id's,
      // it will be returned - otherwise it will be newly created
      return this.termSetCacheDB.createTreeMap(mapId.toString())
          .keySerializer(ByteArray.SERIALIZER_BTREE)
          .valueSerializer(Serializer.LONG)
          .makeOrGet();
    }
  }

  /**
   * Builder for creating a new {@link DirectAccessIndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractIndexDataProviderBuilder<Builder> {

    /**
     * Constructor setting the implementation identifier for the cache.
     */
    public Builder() {
      super(IDENTIFIER);
    }

    @Override
    Builder getThis() {
      return this;
    }

    @Override
    public DirectAccessIndexDataProvider build()
        throws BuildException, ConfigurationException {
      validate();
      try {
        return new DirectAccessIndexDataProvider(this);
      } catch (final DataProviderException e) {
        throw new BuildException("Failed to build instance.", e);
      }
    }
  }
}
