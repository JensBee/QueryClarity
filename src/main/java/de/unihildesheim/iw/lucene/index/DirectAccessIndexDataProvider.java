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
import de.unihildesheim.iw.util.Progress;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
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
import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.Fun;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
  protected final IndexReader idxReader;
  /**
   * Document fields.
   */
  protected final Set<String> fields;
  /**
   * Stopwords in byte representation.
   */
  protected final Set<byte[]> stopwords;

  /**
   * Disk-backed storage for large collections.
   */
  private final DB cache;
  /**
   * Stopwords as {@link String}.
   */
  private final Set<String> stopwordsStr;
  /**
   * Stopwords as {@link ByteArray}.
   */
  private final Set<ByteArray> stopwordsBytes;
  /**
   * Cached index frequency values for single terms.
   */
  protected final BTreeMap<byte[], Long> c_termFreqs;
  /**
   * Cached relative index frequency values for single terms.
   */
  private final BTreeMap<byte[], BigDecimal> c_relTermFreqs;
  /**
   * Cached document frequency values for single terms.
   */
  private final BTreeMap<byte[], Integer> c_docFreqs;
  /**
   * Cached list of document ids that have content in the current field(s) and
   * are not marked as deleted.
   */
  private final BTreeMap<Integer, Object> c_documentIds;
  /**
   * Cached list of all terms currently available in all known documents and
   * fields.
   */
  // accessed from inner classes
  @SuppressWarnings("ProtectedField")
  protected final BTreeMap<byte[], Object> c_idxTerms;
  /**
   * Stores last commit generation of the Lucene index.
   */
  private final Long idxCommitGeneration;
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
  private long idxTermFrequency;
  /**
   * Frequency of all known terms in the index. Cached as {@link BigDecimal}
   * value for calculations.
   */
  private BigDecimal idxTermFrequency_bd;
  /**
   * Flag indicating, if this instance has been closed.
   */
  private boolean isClosed;
  /**
   * Startup log prefix.
   */
  private static final String LOG_STARTINFO = "Startup:";
  /**
   * Caches term sets merged from multiple documents. Mapping is {@code
   * (document ids)} -> {@code (termId, frequency)}. Values in the {@code
   * long[]} array are stored as tuples, where the first long is the termId, the
   * second the frequency value.
   */
  private final Map<int[], long[]> c_termSets;
  /**
   * Mapping of term to an unique id. Used to store term related data.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private final BTreeMap<byte[], Long> c_termIds;
  private final Atomic.Long termIdIndex;
  /**
   * Inverse map of {@link #c_termIds}. Created automatically.
   */
  private final BTreeMap<Long, byte[]> c_termIdsInv;

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
    // create caches
    this.cache = DBMakerUtils.newTempFileDB().make();
    this.stopwords = this.cache.createTreeSet("stopwords")
        .serializer(new BTreeKeySerializer.BasicKeySerializer(
            Serializer.BYTE_ARRAY))
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .make();
    this.c_termFreqs = this.cache.createTreeMap("termFreqCache")
        .keySerializerWrap(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.LONG)
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .make();
    this.c_relTermFreqs = this.cache.createTreeMap("relTermFreqs")
        .keySerializerWrap(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.BASIC)
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .make();
    this.c_docFreqs = this.cache.createTreeMap("docFreqs")
        .keySerializerWrap(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.INTEGER)
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .make();
    this.c_documentIds = this.cache.createTreeMap("documentIds")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
        .valueSerializer(null)
        .counterEnable()
        .make();
    this.c_idxTerms = this.cache.createTreeMap("idxTerms")
        .keySerializerWrap(Serializer.BYTE_ARRAY)
        .valueSerializer(null)
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .counterEnable()
        .make();
    this.c_termSets = this.cache
        .createTreeMap("termSets")
        .keySerializerWrap(Serializer.INT_ARRAY)
        .valueSerializer(Serializer.LONG_ARRAY)
        .comparator(Fun.INT_ARRAY_COMPARATOR)
        .make();
    this.c_termIds = this.cache
        .createTreeMap("termIds")
        .keySerializerWrap(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.LONG)
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .counterEnable()
        .makeOrGet();
    this.termIdIndex = this.cache
        .createAtomicLong("termIdIndex", 0);
    this.c_termIdsInv = this.cache
        .createTreeMap("termIdsInverse")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
        .valueSerializer(Serializer.BYTE_ARRAY)
        .counterEnable()
        .makeOrGet();

    // bind listener to create inverse map on adds to real map
    Bind.mapInverse(this.c_termIds, this.c_termIdsInv);

    // set stopwords
    this.stopwordsStr = new HashSet<>(builder.stopwords.size());
    this.stopwordsStr.addAll(builder.stopwords);
    this.stopwordsBytes = new HashSet<>(builder.stopwords.size());
    for (final String word : this.stopwordsStr) {
      final byte[] wordBytes = word.getBytes(StandardCharsets.UTF_8);
      this.stopwordsBytes.add(new ByteArray(wordBytes));
      this.stopwords.add(wordBytes);
    }

    // set Lucene index reader
    this.idxReader = builder.idxReader;
    this.idxCommitGeneration = builder.lastCommitGeneration;

    // set fields to use
    this.fields = new HashSet<>(builder.documentFields.size());
    this.fields.addAll(builder.documentFields);

    // cache some Lucene values
    this.liveDocs = MultiFields.getLiveDocs(this.idxReader);
    this.idxReaderLeaves = this.idxReader.getContext().leaves();

    try {
      initCache();
    } catch (final IOException | ParseException | ProcessingException e) {
      throw new DataProviderException("Failed to initialize caches.", e);
    }
  }

  /**
   * Get an unique id for an index term.
   *
   * @param term Term to lookup
   * @return Unique id for the given term
   */
  private Long getTermId(final ByteArray term) {
    Long termId = this.c_termIds.get(term.bytes);
    if (termId == null) {
      termId = this.termIdIndex.incrementAndGet();
      final Long oldId = this.c_termIds.putIfAbsent(term.bytes, termId);
      if (oldId != null) {
        termId = oldId;
      }
    }
    return termId;
  }

  /**
   * Initialized values that have to be cached beforehand.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws DataProviderException Wrapped IOException Thrown on low-level I/O
   * errors
   * @throws ParseException Thrown if the query estimating the maximum of
   * matching documents has failed
   */
  private void initCache()
      throws IOException, DataProviderException, ParseException,
             ProcessingException {
    collectDocuments(); // cache all documents
    collectIndexTerms(); // cache all index terms
    calcTermFrequency(); // cache overall index term frequency value
    LOG.info("Compacting cache");
    this.cache.compact();
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
    LOG.info("{} Collecting all documents from index with field(s) {}",
        LOG_STARTINFO, this.fields);
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
        this.c_documentIds.put(doc.doc, IGNORED_MAP_VALUE);
      }
    }

    LOG.info("{} Collecting {} documents from index with field(s) {} took {}",
        LOG_STARTINFO, this.c_documentIds.size(), this.fields,
        tm.stop().getTimeString());
  }

  /**
   * Collects all terms from all currently set fields. This is really slow, if
   * the index is largely fragmented.
   *
   * @throws DataProviderException Wrapped {@link IOException} which gets thrown
   * on low-level I/O errors
   */
  private void collectIndexTerms()
      throws DataProviderException, ProcessingException {
    if (this.stopwords.isEmpty()) {
      LOG.info("{} Collecting all terms from index (fields {})",
          LOG_STARTINFO, this.fields);
    } else {
      LOG.info(
          "{} Collecting all terms from index (fields {}), excluding stopwords",
          LOG_STARTINFO, this.fields);
    }
    final TimeMeasure tm = new TimeMeasure().start();

    if (this.idxReader.leaves().size() > 1) {
      final int segCount = this.idxReader.leaves().size();
      LOG.info("Collecting terms from {} segments.", segCount);
      new Processing().setSourceAndTarget(
          new TargetFuncCall<>(
              new CollectionSource<>(this.idxReader.leaves()),
              new TargetFuncCall.TargetFunc<AtomicReaderContext>() {
                @Override
                public void call(final AtomicReaderContext aContext)
                    throws Exception {
                  if (aContext == null) {
                    return;
                  }
                  Terms terms;
                  TermsEnum termsEnum = TermsEnum.EMPTY;
                  for (final String field : DirectAccessIndexDataProvider.this
                      .fields) {
                    terms = aContext.reader().terms(field);
                    if (terms != null) {
                      termsEnum = terms.iterator(termsEnum);
                      BytesRef term = termsEnum.next();
                      while (term != null) {
                        final byte[] termBytes = BytesRefUtils.copyBytes(term);
                        // skip, if stopword
                        if (!DirectAccessIndexDataProvider.this
                            .stopwords.contains(termBytes) &&
                            termsEnum.seekExact(term)) {
                          // term frequency
                          if (DirectAccessIndexDataProvider.this
                              .c_termFreqs.containsKey(termBytes)) {
                            DirectAccessIndexDataProvider.this
                                .c_termFreqs.put(termBytes,
                                DirectAccessIndexDataProvider.this
                                    .c_termFreqs.get(termBytes) +
                                    termsEnum.totalTermFreq());
                          } else {
                            DirectAccessIndexDataProvider.this
                                .c_termFreqs.put(
                                termBytes, termsEnum.totalTermFreq());
                          }

                          // document frequency
                          if (DirectAccessIndexDataProvider.this
                              .c_docFreqs.containsKey(termBytes)) {
                            DirectAccessIndexDataProvider.this
                                .c_docFreqs.put(termBytes,
                                DirectAccessIndexDataProvider.this
                                    .c_docFreqs.get(termBytes) +
                                    termsEnum.docFreq());
                          } else {
                            DirectAccessIndexDataProvider.this
                                .c_docFreqs.put(
                                termBytes, termsEnum.docFreq());
                          }

                          // terms
                          DirectAccessIndexDataProvider.this
                              .c_idxTerms.put(termBytes, IGNORED_MAP_VALUE);
                        }
                        term = termsEnum.next();
                      }
                    }
                  }
                }
              }
          )
      ).process(segCount);
    } else {
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
              final byte[] termBytes = BytesRefUtils.copyBytes(term);
              // skip, if stopword
              if (!this.stopwords.contains(termBytes) &&
                  termsEnum.seekExact(term)) {
                if (this.c_termFreqs.containsKey(termBytes)) {
                  this.c_termFreqs.put(termBytes,
                      this.c_termFreqs.get(termBytes) +
                          termsEnum.totalTermFreq());
                } else {
                  this.c_termFreqs.put(termBytes, termsEnum.totalTermFreq());
                }

                this.c_idxTerms.put(termBytes, IGNORED_MAP_VALUE);
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
    }

    LOG.info("{} Collecting {} terms from index (fields {}) took {}",
        LOG_STARTINFO, this.c_idxTerms.size(), this.fields,
        tm.stop().getTimeString());
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

  /**
   * Calculate the index term frequency value. This is the sum of all
   * frequencies of all terms in the index (excluding stopwords).
   */
  private void calcTermFrequency() {
    if (this.stopwords.isEmpty()) {
      LOG.info("{} Calculating index term frequency value (fields {})",
          LOG_STARTINFO, this.fields);
    } else {
      LOG.info(
          "{} Calculating index term frequency value (fields {}), " +
              "excluding stopwords", LOG_STARTINFO, this.fields);
    }
    final TimeMeasure tm = new TimeMeasure().start();
    long freq = 0L;

    for (final Long aFreq : this.c_termFreqs.values()) {
      freq += aFreq;
    }

    this.idxTermFrequency = freq;

    LOG.info("{} Calculating index term frequency value ({})"
            + "for fields {} took {}.", LOG_STARTINFO,
        this.idxTermFrequency, this.fields, tm.stop().getTimeString());

    this.idxTermFrequency_bd = BigDecimal.valueOf(this.idxTermFrequency);
  }

  @Override
  public void cacheDocumentModels(final Collection<Integer> docIds) {
    try {
      LOG.info("Caching {} document models. This may take some time.", docIds
          .size());
      getDocumentsTermsMap(docIds);
    } catch (final DataProviderException e) {
      LOG.warn("Caching failed: {}", e);
    }
  }

  @Override
  public long getTermFrequency()
      throws DataProviderException {
    return this.idxTermFrequency;
  }

  @Override
  public void warmUp()
      throws DataProviderException {
    // do nothing here
  }

  @Override
  public Long getTermFrequency(final ByteArray term)
      throws DataProviderException {
    if (this.stopwords.contains(term.bytes)) {
      // stopword - term not available
      return 0L;
    }

    return this.c_termFreqs.get(term.bytes);
  }

  @Override
  public int getDocumentFrequency(final ByteArray term)
      throws DataProviderException {
    if (this.stopwords.contains(term.bytes)) {
      // stopword - term not available
      return 0;
    }

    return this.c_docFreqs.get(term.bytes);

//    // check for a cached value
//    Integer freq = this.c_docFreqs.get(term.bytes);
//    if (freq == null) {
//      // calculate value
//      Terms terms;
//      TermsEnum termsEnum = TermsEnum.EMPTY;
//      final BytesRef termBytes = BytesRefUtils.refFromByteArray(term);
//      final Set<Integer> collectedDocIds = new HashSet<>(500);
//
//      // iterate over all fields
//      DocsEnum docsEnum;
//      for (final String field : this.fields) {
//        try {
//          terms = getMultiTermsInstance(field);
//          if (terms != null) {
//            termsEnum = terms.iterator(termsEnum);
//            if (termsEnum.seekExact(termBytes)) {
//              // docsEnum should never get null, field and term are already
//              // checked
//              docsEnum = getTermDocsEnum(field, termBytes);
//              int docId = docsEnum.nextDoc();
//
//              // iterate over all documents that have the current term in the
//              // current field
//              while (docId != DocIdSetIterator.NO_MORE_DOCS) {
//                collectedDocIds.add(docId);
//                docId = docsEnum.nextDoc();
//              }
//            }
//          }
//        } catch (final IOException e) {
//          throw new DataProviderException(
//              "Caught exception trying to get terms from field '"
//                  + field + "'.", e);
//        }
//      }
//      freq = collectedDocIds.size();
//      this.c_docFreqs.put(term.bytes, freq);
//    }
//    return freq;
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

  /**
   * {@inheritDoc} Once calculated those values are cached for this instance.
   *
   * @param term Term to lookup
   * @return Relative frequency of the given term in the collection
   * @throws DataProviderException
   */
  @SuppressWarnings("ReuseOfLocalVariable")
  @Override
  public BigDecimal getRelativeTermFrequency(final ByteArray term)
      throws DataProviderException {
    // try get cached value
    BigDecimal rTf = this.c_relTermFreqs.get(term.bytes);
    if (rTf != null) {
      return rTf;
    }

    // calc new value
    final Long tf = this.c_termFreqs.get(term.bytes);
    if (tf == null) {
      return BigDecimal.ZERO;
    }
    rTf = BigDecimalCache.get(tf)
        .divide(this.idxTermFrequency_bd, MATH_CONTEXT);
    this.c_relTermFreqs.put(term.bytes, rTf);

    return rTf;
  }

  @Override
  public void close() {
    LOG.info("Closing IndexDataProvider.");
    if (!this.cache.isClosed()) {
      LOG.debug("Releasing cache.");
      this.cache.close();
    }
    this.isClosed = true;
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    return new Iterator<ByteArray>() {
      /**
       * Iterator over {@code byte[]} entries from {@link
       * DirectAccessIndexDataProvider#c_idxTerms}.
       */
      private final Iterator<byte[]> byteIterator =
          DirectAccessIndexDataProvider.this.c_idxTerms.keySet().iterator();

      @Override
      public boolean hasNext() {
        return this.byteIterator.hasNext();
      }

      @Override
      public ByteArray next() {
        return new ByteArray(this.byteIterator.next());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Iterator<Integer> getDocumentIds() {
    return this.c_documentIds.keySet().iterator();
  }

  @Override
  public long getUniqueTermsCount() {
    return this.c_idxTerms.sizeLong();
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
            getMergedDocumentsTermsMap(Collections.singleton(docId))
        ).getModel();
  }

  @Override
  public boolean hasDocument(final int docId) {
    return this.c_documentIds.containsKey(docId);
  }

  @Override
  public Map<ByteArray, Long> getDocumentTerms(final int docId)
      throws DataProviderException {
    if (!hasDocument(docId)) {
      return Collections.emptyMap();
    }
    return getMergedDocumentsTermsMap(Collections.singleton(docId));
  }

  @Override
  public Iterator<Map.Entry<ByteArray, Long>> getDocumentsTerms(
      final Collection<Integer> docIds)
      throws DataProviderException {
    return getMergedDocumentsTermsMap(docIds).entrySet().iterator();
  }

  @Override
  public Set<ByteArray> getDocumentTermsSet(final int docId)
      throws DataProviderException {
    if (!hasDocument(docId)) {
      return Collections.emptySet();
    }
    return getMergedDocumentsTermsMap(Collections.singleton(docId)).keySet();
  }

  @Override
  public Iterator<ByteArray> getDocumentsTermsSet(
      final Collection<Integer> docIds)
      throws DataProviderException {
    return getMergedDocumentsTermsMap(docIds).keySet().iterator();
  }

  @Override
  public long getDocumentCount()
      throws DataProviderException {
    return this.c_documentIds.sizeLong();
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
    return Collections.unmodifiableSet(this.stopwordsBytes);
  }

  @Override
  public boolean isClosed() {
    return this.isClosed;
  }

  private Map<Integer, Map<ByteArray, Long>>getDocumentsTermsMap
      (final Collection<Integer> docIds)
      throws DataProviderException {

    // pre-fill map with maps
    final Map<Integer, Map<ByteArray, Long>> mapMap =
        new HashMap<>((int) (docIds.size() * 0.5));

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

    Map<ByteArray, Long> termsMap;

    if (hasTermVectors) {
      // clean start term maps
      for (final Integer docId : docIds) {
        mapMap.put(docId, new HashMap<ByteArray, Long>());
      }

      try {
        final DocFieldsTermsEnum dftEnum =
            new DocFieldsTermsEnum(this.idxReader, this.fields);
        BytesRef term;
        // step through all fields of each document
        for (final int docId : docIds) {
          dftEnum.setDocument(docId);
          term = dftEnum.next();
          termsMap = mapMap.get(docId);

          // collect terms from document fields
          while (term != null) {
            final ByteArray termBytes = BytesRefUtils.toByteArray(term);
            // skip, if stopword
            if (!this.stopwords.contains(termBytes.bytes)) {
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
      }
    }

    // re-check again, if termVectors are there, but collecting terms has failed
    if (!hasTermVectors) {
      LOG.info("Term vectors not present. Scanning all index terms for {} " +
          "documents.", docIds.size());
      // clean start term maps
      for (final Integer docId : docIds) {
        mapMap.put(docId, new HashMap<ByteArray, Long>());
      }

      // provide status messages
      final Progress progress = new Progress().start();

      Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      BytesRef term;
      DocsEnum docsEnum;
      for (final String field : this.fields) {
        progress.reset(); // reset item count
        LOG.info("Scanning index terms of field {}", field);
        try {
          terms = getMultiTermsInstance(field);
          if (terms != null) {
            termsEnum = terms.iterator(termsEnum);
            term = termsEnum.next();

            while (term != null) {
              progress.inc(); // status
              final ByteArray termBytes = BytesRefUtils.toByteArray(term);
              // skip stopwords and check, if term is there
              if (!this.stopwords.contains(termBytes.bytes) &&
                  termsEnum.seekExact(term)) {
                docsEnum = getTermDocsEnum(field, term);
                int docId = docsEnum.nextDoc();

                final Collection<Integer> docIdsToCheck =
                    new ArrayList<>(docIds);
                while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                  if (docIdsToCheck.contains(docId)) {
                    termsMap = mapMap.get(docId);

                    if (termsMap.containsKey(termBytes)) {
                      termsMap.put(termBytes, termsMap.get(termBytes)
                          + (long) docsEnum.freq());
                    } else {
                      termsMap.put(termBytes, (long) docsEnum.freq());
                    }
                    docIdsToCheck.remove(docId);
                    // test if there's something to check
                    if (docIdsToCheck.isEmpty()) {
                      break;
                    }
                  }
                  docId = docsEnum.nextDoc();
                }
              }

              term = termsEnum.next();
            }
            progress.finished(); // status
          }
        } catch (final IOException e) {
          throw new DataProviderException(
              "Caught exception trying to get terms from field '"
                  + field + "'.", e);
        }
      }
    }

    // create values for caching
    for (final Integer docId : docIds) {
      termsMap = mapMap.get(docId);

      final long[] termsMapValue = new long[termsMap.size() * 2];
      int termsMapEntryCount = 0;
      for (final Entry<ByteArray, Long> tmE : termsMap.entrySet()) {
        termsMapValue[termsMapEntryCount] = getTermId(tmE.getKey());
        termsMapValue[termsMapEntryCount + 1] = tmE.getValue();
        termsMapEntryCount += 2;
      }
      this.c_termSets.put(createTermSetCacheKey(Collections.singleton(docId)),
          termsMapValue);
    }

    return mapMap;
  }

  private static int[] createTermSetCacheKey(final Collection<Integer> docIds) {
    final List<Integer> docIdList = new ArrayList<>(docIds);
    Collections.sort(docIdList);
    final int[] termsMapKey = new int[docIdList.size()];
    for (int i = 0; i < docIdList.size(); i++) {
      termsMapKey[i] = docIdList.get(i);
    }
    return termsMapKey;
  }

  /**
   * Get a mapping of term->frequency for a set of documents
   *
   * @param docIds Document id's to extract the terms from
   * @return Mapping of term->frequency for all specified documents
   * @throws DataProviderException Wrapped {@link IOException} which gets thrown
   * on low-level I/O errors
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  private Map<ByteArray, Long> getMergedDocumentsTermsMap(
      final Collection<Integer> docIds)
      throws DataProviderException {

    // construct the cache key
    /*
    final List<Integer> docIdList = new ArrayList<>(docIds);
    Collections.sort(docIdList);
    final int[] termsMapKey = new int[docIdList.size()];
    for (int i = 0; i < docIdList.size(); i++) {
      termsMapKey[i] = docIdList.get(i);
    }*/
    final int[] termsMapKey = createTermSetCacheKey(docIds);

    final Map<ByteArray, Long> termsMap;

    // try to get a cached map
    final long[] termIds = this.c_termSets.get(termsMapKey);
    if (termIds != null) {
      termsMap = new HashMap<>((int) Math.ceil(
          (double) (termIds.length / 2) / 0.75));
      for (int i = 0; i < termIds.length; i += 2) {
        termsMap.put(
            new ByteArray(this.c_termIdsInv.get(termIds[i])),
            termIds[i + 1]);
      }
      return termsMap;
    }

    // create terms map
    final Map<Integer, Map<ByteArray, Long>> mapMap =
        getDocumentsTermsMap(docIds);

    termsMap = new HashMap<>(500 * docIds.size()); // rough size guess

    for (final Integer docId : docIds) {
      final Map<ByteArray, Long> docTermsMap = mapMap.get(docId);

      for (final Entry<ByteArray, Long> tmE : docTermsMap.entrySet()) {
        if (termsMap.containsKey(tmE.getKey())) {
          termsMap.put(tmE.getKey(),
              termsMap.get(tmE.getKey()) + tmE.getValue());
        } else {
          termsMap.put(tmE.getKey(), tmE.getValue());
        }
      }
    }

    /*
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
    */

    /*
    if (hasTermVectors) {
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
            if (!this.stopwords.contains(termBytes.bytes)) {
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
    */

    /*
    // re-check again, if termVectors are there, but collecting terms has failed
    if (!hasTermVectors) {
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
              final ByteArray termBytes = BytesRefUtils.toByteArray(term);
              // skip stopwords and check, if term is there
              if (!this.stopwords.contains(termBytes.bytes) &&
                  termsEnum.seekExact(term)) {
                docsEnum = getTermDocsEnum(field, term);
                int docId = docsEnum.nextDoc();

                final Collection<Integer> docIdsToCheck = new ArrayList(docIds);
                while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                  if (docIdsToCheck.contains(docId)) {
                    if (termsMap.containsKey(termBytes)) {
                      termsMap.put(termBytes, termsMap.get(termBytes)
                          + (long) docsEnum.freq());
                    } else {
                      termsMap.put(termBytes, (long) docsEnum.freq());
                    }
                    docIdsToCheck.remove(docId);
                    // test if there's something to check
                    if (docIdsToCheck.isEmpty()) {
                      break;
                    }
                  }
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
    }
    */

    // create values for caching
    final long[] termsMapValue = new long[termsMap.size() * 2];
    int termsMapEntryCount = 0;
    for (final Entry<ByteArray, Long> tmE : termsMap.entrySet()) {
      termsMapValue[termsMapEntryCount] = getTermId(tmE.getKey());
      termsMapValue[termsMapEntryCount + 1] = tmE.getValue();
      termsMapEntryCount += 2;
    }
    this.c_termSets.put(termsMapKey, termsMapValue);

    return termsMap;
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
