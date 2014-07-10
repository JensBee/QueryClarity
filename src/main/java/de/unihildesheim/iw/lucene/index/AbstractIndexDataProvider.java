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
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.StringUtils;
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
  private volatile boolean warmed;

  /**
   * Transient cached overall term frequency of the index.
   */
  private volatile Long idxTf;

  /**
   * Transient cached collection of all (non deleted) document-ids.
   */
  private volatile Set<Integer> idxDocumentIds;

  /**
   * Transient cached document-frequency map for all terms in index.
   */
  private volatile ConcurrentNavigableMap<ByteArray, Integer> idxDfMap;

  /**
   * List of stop-words to exclude from term frequency calculations.
   * Byte-encoded list for internal use.
   */
  private volatile Set<ByteArray> stopwords = Collections.emptySet();

  /**
   * List of stop-words to exclude from term frequency calculations. Cached
   * String set.
   */
  private volatile Set<String> stopwordsStr = Collections.emptySet();

  /**
   * Transient cached collection of all index terms.
   */
  private volatile Set<ByteArray> idxTerms;

  /**
   * Persistent cached collection of all index terms mapped by document field.
   * Mapping is {@code (Field, Term)} to {@code Frequency}. Fields are indexed
   * by {@link #cachedFieldsMap}. <br> The frequency value describes the number
   * of times the term is found across all documents in the specific field.
   *
   * @see #cachedFieldsMap
   */
  private volatile ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> idxTermsMap;

  /**
   * Persistent cached collection of all index terms mapped by document-field
   * and document-id. Mapping is {@code (Term, Field, Document)} to {@code
   * Frequency}. Fields are indexed by {@link #cachedFieldsMap}.<br> The
   * frequency value describes the times the term is found in the specific field
   * of the specific document.
   *
   * @see #cachedFieldsMap
   */
  private volatile ConcurrentNavigableMap<Fun.Tuple3<
      ByteArray, SerializableByte, Integer>, Integer> idxDocTermsMap;

  /**
   * List of fields cached by this instance. Mapping of {@code field name} to
   * {@code id} value.
   *
   * @see #cachedFieldsMap
   */
  private volatile Map<String, SerializableByte> cachedFieldsMap;

  /**
   * {@link IndexReader} to access the Lucene index.
   */
  private volatile IndexReader idxReader;

  /**
   * List of document fields to operate on.
   */
  private volatile Set<String> documentFields;

  /**
   * Last commit generation id of the Lucene index.
   */
  private volatile Long indexLastCommitGeneration;

  /**
   * Flag indicating, if this instance is closed.
   */
  private volatile boolean isClosed;

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
  final IndexReader getIndexReader() {
    return this.idxReader;
  }

  /**
   * Set the {@link IndexReader} to use for accessing the Lucene index.
   *
   * @param reader Reader to use
   */
  final void setIndexReader(final IndexReader reader) {
    this.idxReader = Objects.requireNonNull(reader, "IndexReader was null.");
  }

  /**
   * Get the state information, if caches are pre-loaded (warmed).
   *
   * @return True, if caches have been loaded
   */
  final boolean areCachesWarmed() {
    return this.warmed;
  }

  /**
   * Sets the flag indicating that this instance is disposed. This means, all
   * data storages have been closed.
   */
  final void setClosed() {
    this.isClosed = true;
  }

  /**
   * Add a new field name to the list of cached fields.
   *
   * @param field Field name
   */
  final synchronized void addFieldToCacheMap(final String field) {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(field, "Field was null."))) {
      throw new IllegalArgumentException("Field was empty.");
    }

    final Collection<SerializableByte> keys = new HashSet<>(
        this.cachedFieldsMap.values());
    for (byte i = Byte.MIN_VALUE; (int) i < (int) Byte.MAX_VALUE; i++) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final SerializableByte sByte = new SerializableByte(i);
      if (keys.add(sByte)) {
        this.cachedFieldsMap.put(field, sByte);
        break;
      }
    }
  }

  /**
   * Get the mapping of cached fields. The mapping is {@code field name} to
   * {@code Byte value}. The Byte value can be used to identify a field. <br>
   * The collection field is exposed to the caller. All modifications are
   * directly changing the original object.
   *
   * @return Mapping of {@code field name} to {@code Byte} value
   * @see #cachedFieldsMap
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  final Map<String, SerializableByte> getCachedFieldsMap() {
    return this.cachedFieldsMap;
  }

  /**
   * Sets the map of cached fields. <br> The collection is referenced from the
   * caller. All modifications are directly changing the original object.
   *
   * @param newFieldsMap Mapping of {@code FieldName} to {@code Byte} value
   * @see #cachedFieldsMap
   */
  final void setCachedFieldsMap(final Map<String,
      SerializableByte> newFieldsMap) {
    this.cachedFieldsMap = Objects.requireNonNull(newFieldsMap,
        "Cached fields map was null.");
  }

  /**
   * Gets the mapping of {@code (FieldByte, Term)} to field based {@code
   * index-frequency}. <br> The collection field is exposed to the caller. All
   * modifications are directly changing the original object.
   *
   * @return Per field term index-frequency mapping
   * @see #idxTermsMap
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  final ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> getIdxTermsMap() {
    return this.idxTermsMap;
  }

  /**
   * Sets the mapping of {@code (FieldByte, Term)} to field based {@code
   * index-frequency}. <br> The collection is referenced from the caller. All
   * modifications are directly changing the original object.
   *
   * @param newIdxTermsMap Per field term index-frequency mapping
   * @see #idxTermsMap
   */
  final void setIdxTermsMap(final ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> newIdxTermsMap) {
    this.idxTermsMap = Objects.requireNonNull(newIdxTermsMap,
        "Index terms map was null.");
  }

  /**
   * Gets the mapping of {@code (Term, Field, Document)} to {@code in-document
   * frequency}. <br> The collection field is exposed to the caller. All
   * modifications are directly changing the original object.
   *
   * @return Per Field in-document frequency for each term and document in index
   * @see #idxDocTermsMap
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  final synchronized ConcurrentNavigableMap<Fun.Tuple3<
      ByteArray, SerializableByte, Integer>, Integer> getIdxDocTermsMap() {
    return this.idxDocTermsMap;
  }

  /**
   * Sets the mapping of {@code (Term, Field, Document)} to {@code in-document
   * frequency}. <br> The collection is referenced from the caller. All
   * modifications are directly changing the original object.
   *
   * @param newIdxDocTermsMap Data map to reference
   * @see #idxDocTermsMap
   */
  final void setIdxDocTermsMap(final ConcurrentNavigableMap<Fun.Tuple3<
      ByteArray, SerializableByte, Integer>, Integer> newIdxDocTermsMap) {
    this.idxDocTermsMap = newIdxDocTermsMap;
//    this.idxDocTermsMap = Objects.requireNonNull(newIdxDocTermsMap,
//        "Index document term-frequency map was null.");
  }

  /**
   * Clears the index term-frequency value.
   *
   * @see #idxTf
   */
  @SuppressWarnings("AssignmentToNull")
  final void clearIdxTf() {
    this.idxTf = null;
  }

  /**
   * Get the overall term-frequency value for the whole index.
   *
   * @return Frequency of all terms in the index
   * @see #idxTf
   */
  final Long getIdxTf() {
    return this.idxTf;
  }

  /**
   * Set the overall term-frequency value for the whole index.
   *
   * @param tf Frequency of all terms in the index
   * @see #idxTf
   */
  final void setIdxTf(final Long tf) {
    this.idxTf = Objects.requireNonNull(tf, "Term frequency value was null.");
  }

  /**
   * Check, if the given String is flagged as stopword.
   *
   * @param word Word to check
   * @return True, if it's a stopword
   * @throws UnsupportedEncodingException Thrown, if the word could not be
   * encoded to UTF-8 bytes
   */
  protected boolean isStopword(final String word)
      throws UnsupportedEncodingException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(word, "Term was null."))) {
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
  final boolean isStopword(final ByteArray word) {
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
   * @see #isTemporary
   */
  final boolean isTemporary() {
    return this.isTemporary;
  }

  /**
   * Default implementation of {@link #warmUpTerms()}.
   *
   * @return True, if data was loaded and not calculated in place
   * @throws ProcessingException Thrown, if term processing encountered an
   * error
   * @see #idxTerms
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
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
   * @throws ProcessingException Thrown, if term processing encountered an
   * error
   * @see #idxTerms
   */
  @SuppressWarnings("ObjectAllocationInLoop")
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
            this.idxTerms.add(new ByteArray(byteArray));
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
   * @see #cachedFieldsMap
   */
  final SerializableByte getFieldId(final String fieldName) {
    if (this.cachedFieldsMap.containsKey(fieldName)) {
      return this.cachedFieldsMap.get(
          Objects.requireNonNull(fieldName));
    }
    throw new IllegalStateException("Unknown field '" + fieldName
        + "'. No id found.");
  }

  /**
   * Default implementation of {@link #warmUpIndexTermFrequencies()}.
   *
   * @return True, if data was loaded and not calculated in place
   * @see #idxTf
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
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
   *
   * @see #idxTf
   */
  @Override
  public final long getTermFrequency() {
    checkClosed();
    if (this.idxTf == null) {
      LOG.info("Building term frequency index.");
      this.idxTf = 0L;
      long newTf = 0L;

      SerializableByte fieldId;
      for (final String field : this.documentFields) {
        fieldId = getFieldId(field);
        for (final ByteArray bytes : Fun.
            filter(this.idxTermsMap.keySet(), fieldId)) {
          newTf += this.idxTermsMap.get(Fun.t2(fieldId, bytes));
        }
      }

      // remove term frequencies of stop-words
      if (!this.stopwords.isEmpty()) {
        Long tf;
        for (final ByteArray stopWord : this.stopwords) {
          tf = getTermFrequencyIgnoringStopwords(stopWord);
          if (tf != null) {
            newTf -= tf;
          }
        }
      }
      this.idxTf = newTf;
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
  public void warmUp()
      throws DataProviderException {
    checkClosed();
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
   *
   * @throws DataProviderException Wraps any exception thrown during processing
   */
  protected abstract void warmUpTerms()
      throws DataProviderException;

  /**
   * Pre-cache term frequencies.
   */
  protected abstract void warmUpIndexTermFrequencies();

  /**
   * Pre-cache document-ids.
   */
  protected abstract void warmUpDocumentIds();

  /**
   * Pre-cache term-document frequencies.
   *
   * @throws DataProviderException Wraps any exception thrown during processing
   */
  abstract void warmUpDocumentFrequencies()
      throws DataProviderException;

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public final Long getTermFrequency(final ByteArray term) {
    checkClosed();
    if (this.stopwords.contains(Objects.requireNonNull(term,
        "Term was null."))) {
      // skip stop-words
      return 0L;
    }
    return getTermFrequencyIgnoringStopwords(term);
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public final double getRelativeTermFrequency(final ByteArray term) {
    checkClosed();
    if (this.stopwords.contains(Objects.requireNonNull(term,
        "Term was null."))) {
      // skip stop-words
      return 0d;
    }

    final double tf = getTermFrequency(term).doubleValue();
    if (tf == 0) {
      return 0d;
    }
    return tf / (double) getTermFrequency();
  }

  @Override
  public final Iterator<ByteArray> getTermsIterator()
      throws DataProviderException {
    checkClosed();
    try {
      return getTerms().iterator();
    } catch (final ProcessingException e) {
      throw new DataProviderException("Failed to get terms iterator.", e);
    }
  }

  @Override
  public final Source<ByteArray> getTermsSource()
      throws ProcessingException {
    checkClosed();
    return new CollectionSource<>(getTerms());
  }

  @Override
  public final Iterator<Integer> getDocumentIdIterator() {
    checkClosed();
    return getDocumentIds().iterator();
  }

  @Override
  public final Source<Integer> getDocumentIdSource() {
    checkClosed();
    return new CollectionSource<>(getDocumentIds());
  }

  @Override
  public final long getUniqueTermsCount()
      throws DataProviderException {
    checkClosed();
    try {
      return (long) getTerms().size();
    } catch (final ProcessingException e) {
      throw new DataProviderException("Failed to get unique terms count.", e);
    }
  }

  @Override
  public final boolean hasDocument(final int docId) {
    checkClosed();
    if (this.idxDocumentIds == null) {
      throw new IllegalStateException("No document ids set.");
    }
    return this.idxDocumentIds.contains(docId);
  }

  @Override
  public final long getDocumentCount() {
    checkClosed();
    return (long) getDocumentIds().size();
  }

  @Override
  public final Long getLastIndexCommitGeneration() {
    checkClosed();
    return this.indexLastCommitGeneration;
  }

  @Override
  public final Set<String> getDocumentFields() {
    checkClosed();
    return Collections.unmodifiableSet(this.documentFields);
  }

  /**
   * Set the list of document fields to operate on. <br> The collection directly
   * referenced from the caller. All modifications are directly changing the
   * original object.
   *
   * @param fields List of document field names
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  final void setDocumentFields(final Set<String> fields) {
    if (Objects.requireNonNull(fields, "Fields were null.").isEmpty()) {
      throw new IllegalArgumentException("Field list was empty.");
    }
    this.documentFields = fields;
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  @Override
  public final Set<String> getStopwords() {
    checkClosed();
    return this.stopwordsStr;
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  @Override
  public Set<ByteArray> getStopwordsBytes() {
    checkClosed();
    return this.stopwords;
  }

  @Override
  public final boolean isClosed() {
    return this.isClosed;
  }

  /**
   * Set the (String) list of stopwords.
   *
   * @param words List of words to exclude
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not be
   * encoded into the target charset (usually UTF-8)
   */
  final void setStopwords(final Set<String> words)
      throws UnsupportedEncodingException {
    this.stopwordsStr = Objects.requireNonNull(words, "Stopwords were null.");
    this.stopwords = new HashSet<>(this.stopwordsStr.size());
    for (final String word : words) {
      // terms in Lucene are UTF-8 encoded
      @SuppressWarnings("ObjectAllocationInLoop")
      final ByteArray termBa = new ByteArray(word.getBytes("UTF-8"));
      this.stopwords.add(termBa);
    }
  }

  /**
   * Set the last Lucene index commit generation id.
   *
   * @param cGen Generation id
   */
  final void setLastIndexCommitGeneration(final Long cGen) {
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
   * Checks, if this instance has been closed. Throws a runtime exception, if
   * the instance has already been closed.
   */
  final void checkClosed() {
    if (this.isClosed) {
      throw new IllegalStateException("Instance has been closed.");
    }
  }

  /**
   * Get the term frequency including stop-words.
   *
   * @param term Term to lookup
   * @return Term frequency
   */
  final long getTermFrequencyIgnoringStopwords(final ByteArray term) {
    Objects.requireNonNull(term, "Term was null.");

    long tf = 0L;
    Long fieldTf;
    for (final String field : this.documentFields) {
      fieldTf = this.idxTermsMap.get(Fun.t2(getFieldId(field), term));
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
   * @see #documentFields
   */
  final Collection<SerializableByte> getDocumentFieldIds() {
    final Collection<SerializableByte> fieldIds =
        new HashSet<>(this.documentFields
            .size());
    for (final String fieldName : this.documentFields) {
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
   *
   * @throws ProcessingException Thrown, if calculating document frequencies in
   * place encountered an error
   */
  final void warmUpDocumentFrequencies_default()
      throws ProcessingException {
    // cache document frequency values for each term
    if (this.idxDfMap.isEmpty()) {
      final TimeMeasure tStep = new TimeMeasure().start();
      LOG.info("Cache warming: document frequencies..");

      if (this.idxTerms.isEmpty()) {
        LOG.warn("No index terms found.");
      } else {
        new Processing(new TargetFuncCall<>(
            new CollectionSource<>(this.idxTerms),
            new DocumentFrequencyCollectorTarget()
        )).process(this.idxTerms.size());
      }

      LOG.info("Cache warming: calculating document frequencies "
              + "for {} documents and {} terms took {}.",
          this.idxDocumentIds.size(), this.idxTerms.size(),
          tStep.stop().getTimeString()
      );
    } else {
      LOG.info("Cache warming: Document frequencies "
              + "for {} documents and {} terms already loaded.",
          this.idxDocumentIds.size(), this.idxTerms.size()
      );
    }
  }

  /**
   * Get a mapping of {@code Term} to {@code DocumentFrequency}. <br> The
   * collection field is exposed to the caller. All modifications are directly
   * changing the original object.
   *
   * @return Document frequency by term
   * @see #idxDfMap
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  final ConcurrentNavigableMap<ByteArray, Integer> getIdxDfMap() {
    return this.idxDfMap;
  }

  /**
   * Set the index {@code Term} to {@code DocumentFrequency} mapping. <br> The
   * collection is directly referenced from the caller. All modifications are
   * directly changing the original object.
   *
   * @param map Document-frequency by term
   */
  final void setIdxDfMap(final ConcurrentNavigableMap<ByteArray,
      Integer> map) {
    this.idxDfMap = Objects.requireNonNull(map, "Index document-frequency map" +
        " was null.");
  }

  /**
   * Get the index terms list. <br> The collection field is exposed to the
   * caller. All modifications are directly changing the original object.
   *
   * @return Set of terms in the index
   */
  @SuppressWarnings({"ReturnOfCollectionOrArrayField", "TypeMayBeWeakened"})
  final Set<ByteArray> getIdxTerms() {
    return this.idxTerms;
  }

  /**
   * Set the index terms list. <br> The collection is directly referenced from
   * the caller. All modifications are directly changing the original object.
   *
   * @param newIdxTerms Set of terms in the index
   */
  final void setIdxTerms(final Set<ByteArray> newIdxTerms) {
    this.idxTerms = Objects.requireNonNull(newIdxTerms,
        "Index term set was null.");
  }

  /**
   * Get the list of document-ids from the index. <br> The collection field is
   * exposed to the caller. All modifications are directly changing the original
   * object.
   *
   * @return Set of document ids
   */
  @SuppressWarnings({"ReturnOfCollectionOrArrayField", "TypeMayBeWeakened"})
  final Set<Integer> getIdxDocumentIds() {
    return this.idxDocumentIds;
  }

  /**
   * Set the list of document-ids from the index. <br> The collection is
   * directly referenced from the caller. All modifications are directly
   * changing the original object.
   *
   * @param docIds Set of document ids
   */
  final void setIdxDocumentIds(final Set<Integer> docIds) {
    this.idxDocumentIds = Objects.requireNonNull(docIds,
        "Document ids were null.");
  }

  /**
   * {@link Processing} {@link Target} for collecting document frequency values
   * based on terms.
   */
  private final class DocumentFrequencyCollectorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    /**
     * List of ids linked with currently active document fields.
     *
     * @see #cachedFieldsMap
     */
    private final Collection<SerializableByte> docFieldIds;

    /**
     * Create a new Target collecting document frequency values. The currently
     * known fields and term will be used for calculation.
     */
    DocumentFrequencyCollectorTarget() {
      this.docFieldIds = getDocumentFieldIds();
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        @SuppressWarnings("CollectionWithoutInitialCapacity")
        final Set<Integer> matchedDocs = new HashSet<>();

        for (final SerializableByte fieldId : this.docFieldIds) {
          // iterate only a reduced set of documents
          for (final Integer docId :
              Fun.filter(getIdxDocTermsMap().keySet(), term, fieldId)) {
            matchedDocs.add(docId);
          }
        }

        assert !matchedDocs.isEmpty();
        getIdxDfMap().put(new ByteArray(term), matchedDocs.size());
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
     * Accessor for parent class.
     */
    TermCollectorTarget() {
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    @Override
    public void call(final String fieldName) {
      if (fieldName != null) {
        for (final ByteArray byteArray : Fun
            .filter(getIdxTermsMap().keySet(), getFieldId(fieldName))) {
          getIdxTerms().add(new ByteArray(byteArray));
        }
      }
    }
  }
}
