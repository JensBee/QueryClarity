/*
 * Copyright (C) 2014 Jens Bertram
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
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Abstract implementation of {@link IndexDataProvider} supporting {@link
 * Persistence} storage.
 *
 * @author Jens Bertram
 */
abstract class AbstractIndexDataProvider
    implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractIndexDataProvider.class);
  /**
   * Flag indicating, if this instance is temporary (no data is hold
   * persistent).
   */
  private final boolean isTemporary;
  /**
   * Flag indicating, if caches are filled (warmed).
   */
  private boolean warmed;
  /**
   * Transient cached overall term frequency of the index.
   */
  private Long idxTf;
  /**
   * Transient cached collection of all (non deleted) document-ids.
   */
  private Set<Integer> idxDocumentIds;
  /**
   * Transient cached document-frequency map for all terms in index.
   */
  private ConcurrentNavigableMap<ByteArray, Integer> idxDfMap;
  /**
   * List of stop-words to exclude from term frequency calculations.
   * Byte-encoded list for internal use.
   */
  private Set<ByteArray> stopwords = Collections.<ByteArray>emptySet();
  /**
   * List of stop-words to exclude from term frequency calculations. Cached
   * String set.
   */
  private Set<String> stopwordsStr = Collections.<String>emptySet();
  /**
   * Transient cached collection of all index terms.
   */
  private Set<ByteArray> idxTerms;
  /**
   * Persistent cached collection of all index terms mapped by document field.
   * Mapping is {@code(Field, Term)} to {@code Frequency}. Fields are indexed by
   * {@link #cachedFieldsMap}.
   */
  private ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> idxTermsMap;
  /**
   * Persistent cached collection of all index terms mapped by document-field
   * and document-id. Mapping is {@code(Term, Field, Document)} to {@code
   * Frequency}. Fields are indexed by {@link #cachedFieldsMap}.
   */
  private ConcurrentNavigableMap<Fun.Tuple3<
      ByteArray, SerializableByte, Integer>, Integer> idxDocTermsMap;
  /**
   * List of fields cached by this instance. Mapping of field name to id value.
   */
  private Map<String, SerializableByte> cachedFieldsMap;
  /**
   * {@link IndexReader} to access the Lucene index.
   */
  private IndexReader idxReader;

  /**
   * List of document fields to operate on.
   */
  private Set<String> documentFields;

  /**
   * Last commit generation id of the Lucene index.
   */
  private Long indexLastCommitGeneration;

  /**
   * Flag indicating, if this instance is closed.
   */
  private transient boolean isDisposed;

  /**
   * Initializes the abstract instance.
   *
   * @param isTemp If true, all data will be deleted after terminating the JVM.
   */
  AbstractIndexDataProvider(final boolean isTemp) {
    this.isTemporary = isTemp;
    if (this.isTemporary) {
      LOG.info("Caches are temporary!");
    }
  }

  /**
   * Get the {@link IndexReader} to use for accessing the Lucene index.
   *
   * @return Reader
   */
  protected IndexReader getIndexReader() {
    return this.idxReader;
  }

  /**
   * Set the {@link IndexReader} to use for accessing the Lucene index.
   *
   * @param reader Reader to use
   */
  protected void setIndexReader(final IndexReader reader) {
    this.idxReader = Objects.requireNonNull(reader, "IndexReader was null.");
  }

  /**
   * Get the state information, if caches are pre-loaded (warmed).
   *
   * @return True, if caches have been loaded
   */
  protected boolean cachesWarmed() {
    return this.warmed;
  }

  protected void setDisposed() {
    this.isDisposed = true;
  }

  protected void addFieldToCacheMap(final String field) {
    if (Objects.requireNonNull(field, "Field was null.").trim().isEmpty()) {
      throw new IllegalArgumentException("Field was empty.");
    }

    final Collection<SerializableByte> keys = new HashSet<>(
        this.cachedFieldsMap.values());
    for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
      final SerializableByte sByte = new SerializableByte(i);
      if (!keys.contains(sByte)) {
        getCachedFieldsMap().put(field, sByte);
        keys.add(sByte);
        break;
      }
    }
  }

  protected Map<String, SerializableByte> getCachedFieldsMap() {
    return this.cachedFieldsMap;
  }

  protected void setCachedFieldsMap(final Map<String,
      SerializableByte> newFieldsMap) {
    this.cachedFieldsMap = Objects.requireNonNull(newFieldsMap,
        "Cached fields map was null.");
  }

  protected ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> getIdxTermsMap() {
    return this.idxTermsMap;
  }

  protected void setIdxTermsMap(final ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> newIdxTermsMap) {
    this.idxTermsMap = Objects.requireNonNull(newIdxTermsMap,
        "Index terms map was null.");
  }

  protected ConcurrentNavigableMap<Fun.Tuple3<
      ByteArray, SerializableByte, Integer>, Integer> getIdxDocTermsMap() {
    return this.idxDocTermsMap;
  }

  protected void setIdxDocTermsMap(final ConcurrentNavigableMap<Fun.Tuple3<
      ByteArray, SerializableByte, Integer>, Integer> newIdxDocTermsMap) {
    this.idxDocTermsMap = Objects.requireNonNull(newIdxDocTermsMap,
        "Index document term-frequency map was null.");
  }

  protected void clearIdxTf() {
    this.idxTf = null;
  }

  protected Long getIdxTf() {
    return this.idxTf;
  }

  protected void setIdxTf(final Long tf) {
    this.idxTf = Objects.requireNonNull(tf, "Term frequency value was null.");
  }

  /**
   * Checks, if the given String is flagged as stopword.
   *
   * @param word Word to check
   * @return True, if it's a stopword
   */
  protected boolean isStopword(final String word)
      throws UnsupportedEncodingException {
    if (Objects.requireNonNull(word, "Term was null.").trim().isEmpty()) {
      throw new IllegalArgumentException("Term was empty.");
    }
    return isStopword(new ByteArray(word.getBytes("UTF-8")));
  }

  /**
   * Checks, if the given {@link ByteArray} is flagged as stopword.
   *
   * @param word Word to check
   * @return True, if it's a stopword
   */
  protected boolean isStopword(final ByteArray word) {
    return this.stopwords.contains(Objects.requireNonNull(word,
        "Term was null."));
  }

  /**
   * Checks, if the given {@link BytesRef} is flagged as stopword.
   *
   * @param word Word to check
   * @return True, if it's a stopword
   */
  protected boolean isStopword(final BytesRef word) {
    return isStopword(BytesRefUtils.toByteArray(Objects.requireNonNull(word,
        "Term was null.")));
  }

  /**
   * Checks if this instance is temporary.
   *
   * @return True, if temporary
   */
  final boolean isTemporary() {
    return this.isTemporary;
  }

  /**
   * Default implementation of {@link #warmUpTerms()}.
   */
  final boolean warmUpTerms_default()
      throws ProcessingException {
    final boolean loaded;
    final TimeMeasure tStep = new TimeMeasure();
    // cache all index terms
    if (this.idxTerms == null || this.idxTerms.isEmpty()) {
      loaded = false;
      tStep.start();
      LOG.info("Cache warming: terms..");
      getTerms(); // caches this.idxTerms
      LOG.info("Cache warming: collecting {} unique terms took {}.",
          this.idxTerms.size(), tStep.stop().getTimeString());
    } else {
      loaded = true;
      LOG.info("Cache warming: {} Unique terms already loaded.",
          this.idxTerms.size());
    }
    return loaded;
  }

  /**
   * Collect and cache all index terms. Stop-words will be removed from the
   * list.
   *
   * @return Unique collection of all terms in index with stopwords removed
   */
  final Collection<ByteArray> getTerms()
      throws ProcessingException {
    if (this.idxTerms.isEmpty()) {
      LOG.info("Building transient index term cache.");

      if (this.documentFields.size() > 1) {
        new Processing(new TargetFuncCall<>(
            new CollectionSource<>(this.documentFields),
            new TermCollectorTarget()
        )).process(this.documentFields.size());
      } else {
        for (final String field : this.documentFields) {
          for (final ByteArray byteArray : Fun.filter(this.idxTermsMap.keySet(),
              getFieldId(field))) {
            this.idxTerms.add(byteArray.clone());
          }
        }
      }
      this.idxTerms.removeAll(this.stopwords);
    }
    return Collections.unmodifiableCollection(this.idxTerms);
  }

  /**
   * Get the id for a named field.
   *
   * @param fieldName Field name
   * @return Id of the field
   */
  final SerializableByte getFieldId(final String fieldName) {
    if (Objects.requireNonNull(fieldName).trim().isEmpty()) {
      throw new IllegalArgumentException("Field name was empty.");
    }

    final SerializableByte fieldId = this.cachedFieldsMap.get(fieldName);
    if (fieldId == null) {
      throw new IllegalStateException("Unknown field '" + fieldName
          + "'. No id found.");
    }
    return fieldId;
  }

  /**
   * Default implementation of {@link #warmUpIndexTermFrequencies()}.
   */
  final boolean warmUpIndexTermFrequencies_default() {
    final boolean loaded;
    final TimeMeasure tStep = new TimeMeasure();
    // collect index term frequency
    if (this.idxTf == null || this.idxTf == 0) {
      loaded = false;
      tStep.start();
      LOG.info("Cache warming: index term frequencies..");
      getTermFrequency(); // caches this.idxTf
      LOG.info("Cache warming: index term frequency calculation "
              + "for {} terms took {}.", this.idxTerms.size(),
          tStep.stop().
              getTimeString()
      );
    } else {
      loaded = true;
      LOG.info("Cache warming: "
              + "{} index term frequency values already loaded.",
          this.idxTf
      );
    }
    return loaded;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped.
   */
  @Override
  public final long getTermFrequency() {
    if (this.idxTf == null) {
      LOG.info("Building term frequency index.");
      this.idxTf = 0L;

      SerializableByte fieldId;
      for (final String field : this.documentFields) {
        fieldId = getFieldId(field);
        for (final ByteArray bytes : Fun.
            filter(this.idxTermsMap.keySet(), fieldId)) {
          this.idxTf += this.idxTermsMap.get(Fun.t2(fieldId, bytes));
        }
      }

      // remove term frequencies of stop-words
      if (!this.stopwords.isEmpty()) {
        Long tf;
        for (final ByteArray stopWord : this.stopwords) {
          tf = _getTermFrequency(stopWord);
          if (tf != null) {
            this.idxTf -= tf;
          }
        }
      }
    }
    return this.idxTf;
  }

  /**
   * Default warm-up method calling all warmUp* methods an tracks their running
   * time.
   *
   * @throws DataProviderException
   */
  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public void warmUp()
      throws DataProviderException {
    if (this.warmed) {
      LOG.info("Caches are up to date.");
      return;
    }

    final TimeMeasure tOverAll = new TimeMeasure().start();

    // order matters!
    warmUpTerms(); // should be first
    warmUpIndexTermFrequencies(); // may need terms
    warmUpDocumentIds();
    warmUpDocumentFrequencies(); // need terms

    LOG.info("Cache warmed. Took {}.", tOverAll.stop().getTimeString());
    this.warmed = true;
  }

  /**
   * Pre-cache index terms.
   */
  protected abstract void warmUpTerms()
      throws DataProviderException;

  /**
   * Pre-cache term frequencies.
   */
  protected abstract void warmUpIndexTermFrequencies()
      throws DataProviderException;

  /**
   * Pre-cache document-ids.
   */
  protected abstract void warmUpDocumentIds()
      throws DataProviderException;

  /**
   * Pre-cache term-document frequencies.
   */
  abstract void warmUpDocumentFrequencies()
      throws DataProviderException;

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public final Long getTermFrequency(final ByteArray term) {
    if (this.stopwords.contains(Objects.requireNonNull(term,
        "Term was null."))) {
      // skip stop-words
      return 0L;
    }
    return _getTermFrequency(term);
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public final double getRelativeTermFrequency(final ByteArray term) {
    if (this.stopwords.contains(Objects.requireNonNull(term,
        "Term was null."))) {
      // skip stop-words
      return 0d;
    }

    final double tf = getTermFrequency(term).doubleValue();
    if (tf == 0) {
      return 0d;
    }
    return tf / Long.valueOf(getTermFrequency()).doubleValue();
  }

  @Override
  public final Iterator<ByteArray> getTermsIterator()
      throws DataProviderException {
    try {
      return getTerms().iterator();
    } catch (ProcessingException e) {
      throw new DataProviderException("Failed to get terms iterator.", e);
    }
  }

  @Override
  public final Source<ByteArray> getTermsSource()
      throws ProcessingException {
    return new CollectionSource<>(getTerms());
  }

  @Override
  public final Iterator<Integer> getDocumentIdIterator() {
    return getDocumentIds().iterator();
  }

  @Override
  public final Source<Integer> getDocumentIdSource() {
    return new CollectionSource<>(getDocumentIds());
  }

  @Override
  public final long getUniqueTermsCount()
      throws DataProviderException {
    try {
      return getTerms().size();
    } catch (ProcessingException e) {
      throw new DataProviderException("Failed to get unique terms count.", e);
    }
  }

  @Override
  public final boolean hasDocument(final int docId) {
    if (this.idxDocumentIds == null) {
      throw new IllegalStateException("No document ids set.");
    }
    return this.idxDocumentIds.contains(docId);
  }

  @Override
  public final long getDocumentCount() {
    return getDocumentIds().size();
  }

  @Override
  public Long getLastIndexCommitGeneration() {
    return this.indexLastCommitGeneration;
  }

  @Override
  public Set<String> getDocumentFields() {
    return Collections.unmodifiableSet(this.documentFields);
  }

  /**
   * Set the list of document fields to operate on.
   *
   * @param fields List of document field names
   */
  protected void setDocumentFields(final Set<String> fields) {
    if (Objects.requireNonNull(fields, "Fields were null.").isEmpty()) {
      throw new IllegalArgumentException("Field list was empty.");
    }
    this.documentFields = fields;
  }

  @Override
  public Set<String> getStopwords() {
    return this.stopwordsStr;
  }

  /**
   * Set the (String) list of stopwords.
   *
   * @param words List of words to exclude
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not be
   * encoded into the target charset (usually UTF-8)
   */
  protected void setStopwords(final Set<String> words)
      throws UnsupportedEncodingException {
    this.stopwordsStr = Objects.requireNonNull(words, "Stopwords were null.");
    this.stopwords = new HashSet<>();
    for (final String word : words) {
      // terms in Lucene are UTF-8 encoded
      final ByteArray termBa = new ByteArray(word.getBytes("UTF-8"));
      this.stopwords.add(termBa);
    }
  }

  @Override
  public boolean isDisposed() {
    return this.isDisposed;
  }

  /**
   * Set the last Lucene index commit generation id.
   *
   * @param cGen Generation id
   */
  protected void setLastIndexCommitGeneration(final Long cGen) {
    this.indexLastCommitGeneration = Objects.requireNonNull(cGen,
        "Commit generation was null.");
  }

  /**
   * Collect and cache all document-ids from the index.
   *
   * @return Unique collection of all document ids
   */
  protected abstract Collection<Integer> getDocumentIds();

  /**
   * Get the term frequency including stop-words.
   *
   * @param term Term to lookup
   * @return Term frequency
   */
  @SuppressWarnings("checkstyle:methodname")
  final Long _getTermFrequency(final ByteArray term) {
    Objects.requireNonNull(term, "Term was null.");

    Long tf = 0L;
    for (final String field : this.documentFields) {
      final Long fieldTf = this.idxTermsMap.get(Fun.t2(getFieldId(field),
          term));
      if (fieldTf != null) {
        tf += fieldTf;
      }
    }
    return tf;
  }

  /**
   * Get a list of field-ids of all available document fields.
   *
   * @return List of field ids
   */
  private Set<SerializableByte> getDocumentFieldIds() {
    final Set<SerializableByte> fieldIds = new HashSet<>(this.documentFields
        .size());
    for (String fieldName : this.documentFields) {
      fieldIds.add(getFieldId(fieldName));
    }
    assert !fieldIds.isEmpty();
    return fieldIds;
  }

  /**
   * Default implementation of {@link #warmUpDocumentIds()}.
   */
  final void warmUpDocumentIds_default() {
    final TimeMeasure tStep = new TimeMeasure();
    // cache document ids
    if (this.idxDocumentIds == null || this.idxDocumentIds.isEmpty()) {
      tStep.start();
      LOG.info("Cache warming: documents..");
      getDocumentIds(); // caches this.idxDocumentIds
      if (this.idxDocumentIds != null) {
        LOG.info("Cache warming: collecting {} documents took {}.",
            this.idxDocumentIds.size(),
            tStep.stop().getTimeString());
      }
    } else {
      LOG.info("Cache warming: {} documents already collected.",
          this.idxDocumentIds.size());
    }
  }

  /**
   * Default implementation of {@link #warmUpDocumentFrequencies()}.
   */
  final void warmUpDocumentFrequencies_default()
      throws ProcessingException {
    // cache document frequency values for each term
    if (getIdxDfMap().isEmpty()) {
      final TimeMeasure tStep = new TimeMeasure().start();
      LOG.info("Cache warming: document frequencies..");

      new Processing(new TargetFuncCall<>(
          new CollectionSource<>(getIdxTerms()),
          new DocumentFrequencyCollectorTarget()
      )).process(getIdxTerms().size());

      LOG.info("Cache warming: calculating document frequencies "
              + "for {} documents and {} terms took {}.",
          getIdxDocumentIds().size(), getIdxTerms().size(),
          tStep.stop().getTimeString()
      );
    } else {
      LOG.info("Cache warming: Document frequencies "
              + "for {} documents and {} terms already loaded.",
          getIdxDocumentIds().size(), getIdxTerms().size()
      );
    }
  }

  protected ConcurrentNavigableMap<ByteArray, Integer> getIdxDfMap() {
    return this.idxDfMap;
  }

  /**
   * Get the index terms list.
   *
   * @return
   */
  protected Set<ByteArray> getIdxTerms() {
    return this.idxTerms;
  }

  /**
   * Set the index terms list.
   *
   * @param newIdxTerms
   */
  protected void setIdxTerms(final Set<ByteArray> newIdxTerms) {
    this.idxTerms = Objects.requireNonNull(newIdxTerms,
        "Index term set was null.");
  }

  protected Set<Integer> getIdxDocumentIds() {
    return this.idxDocumentIds;
  }

  protected void setIdxDocumentIds(final Set<Integer> docIds) {
    this.idxDocumentIds = Objects.requireNonNull(docIds,
        "Document ids were null.");
  }

  protected void setIdxDfMap(final ConcurrentNavigableMap<ByteArray,
      Integer> map) {
    this.idxDfMap = Objects.requireNonNull(map, "Index document-frequency map" +
        " was null.");
  }

  /**
   * {@link Processing} {@link Target} for collecting document frequency values
   * based on terms.
   */
  private final class DocumentFrequencyCollectorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    private final Collection<Integer> docIds;
    private final Collection<SerializableByte> docFieldIds;
    private final NavigableSet<Fun.Tuple3<
        ByteArray, SerializableByte, Integer>> docFieldTermSet;

    DocumentFrequencyCollectorTarget()
        throws ProcessingException {
      super();
      this.docIds = getDocumentIds();
      this.docFieldIds = getDocumentFieldIds();
      this.docFieldTermSet = getIdxDocTermsMap().keySet();
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        final Set<Integer> matchedDocs = new HashSet<>();

        for (final SerializableByte fieldId : this.docFieldIds) {
          // iterate only a reduced set of documents
          for (final Integer docId :
              Fun.filter(this.docFieldTermSet, term, fieldId)) {
            matchedDocs.add(docId);
          }
        }

        assert !matchedDocs.isEmpty();
        getIdxDfMap().put(term, matchedDocs.size());
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for collecting currently available index
   * terms.
   */
  private final class TermCollectorTarget
      extends TargetFuncCall.TargetFunc<String> {

    /**
     * Set to get terms from.
     */
    private final NavigableSet<Fun.Tuple2<SerializableByte, ByteArray>>
        terms;

    /**
     * Create a new {@link Processing} {@link Target} for collecting index
     * terms.
     */
    private TermCollectorTarget() {
      super();
      this.terms = getIdxTermsMap().keySet();
    }

    @Override
    public void call(final String fieldName) {
      if (fieldName != null) {
        for (final ByteArray byteArray : Fun.filter(this.terms, getFieldId(
            fieldName))) {
          getIdxTerms().add(byteArray.clone());
        }
      }
    }
  }
}
