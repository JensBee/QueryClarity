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
package de.unihildesheim.lucene.index;

import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.StringUtils;
import de.unihildesheim.util.Tuple;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Source;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Temporary index of random generated documents for testing purposes.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TestIndex implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          TestIndex.class);

  /**
   * Index field names.
   */
  private static List<String> fields;

  /**
   * Bit-mask storing active fields state. 0 means off 1 on. Index is related
   * to {@link #fields}.
   */
  private final int[] activeFieldState;

  /**
   * Flag indicating, if all static fields have been initialized.
   */
  private static boolean initialized = false;

  /**
   * Temporary Lucene index held in memory.
   */
  private static TempDiskIndex tmpIdx;

  /**
   * Minimum and maximum amount of fields to create.
   */
  private static final int[] MIN_MAX_FIELDS = new int[]{1, 10};

  /**
   * Minimum and maximum number of documents to create.
   */
  private static final int[] MIN_MAX_DOCS = new int[]{10, 100};

  /**
   * Minimum and maximum length of a random generated document (in terms per
   * field).
   */
  private static final int[] MIN_MAX_DOC_LENGTH = new int[]{10, 150};

  /**
   * Minimum and maximum length of a random term in a document.
   */
  private static final int[] MIN_MAX_TERM_LENGTH = new int[]{1, 10};

  /**
   * Minimum and maximum length of a random generated query.
   */
  private static final int[] MIN_MAX_QUERY_LENGTH = new int[]{1, 10};

  /**
   * Map: <tt>(prefix -> ([document-id, term, key] -> value))</tt>.
   */
  private Map<String, Map<Tuple.Tuple3<
          Integer, BytesWrap, String>, Object>> prefixMap;

//  private static DB db;

  private int documentsCount;

  /**
   * Field, Document-id, Term -> Frequency
   */
  private static BTreeMap<Fun.Tuple3<Integer, Integer, BytesWrap>, Long> idx;

  /**
   * Initialize the testing index.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  @SuppressWarnings("CollectionWithoutInitialCapacity")
  public TestIndex() throws IOException {
    if (!initialized) {
      createIndex();
    }
    this.activeFieldState = new int[fields.size()];
    // set all fields active
    Arrays.fill(this.activeFieldState, 1);

    this.prefixMap = new ConcurrentHashMap<>();
  }

  /**
   * Create a simple test index and initialize the {@link TempDiskIndex}.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  private void createIndex() throws IOException {
    idx = DBMaker.newTempTreeMap();

    // generate random document fields
    final int fieldsCount = RandomValue.getInteger(MIN_MAX_FIELDS[0],
            MIN_MAX_FIELDS[1]);
    fields = new ArrayList<>(fieldsCount);
    for (int i = 0; i < fieldsCount; i++) {
      fields.add(i + "_" + RandomValue.getString(3, 10));
    }

    // create the lucene index
    tmpIdx = new TempDiskIndex(fields.toArray(new String[fields.size()]));

    // set the number of random documents to create
    documentsCount = RandomValue.getInteger(MIN_MAX_DOCS[0],
            MIN_MAX_DOCS[1]);

    LOG.info("Creating index with {} documents, {} fields each "
            + "and a maximum of {} terms per field. This may take some time.",
            documentsCount, fieldsCount, MIN_MAX_DOC_LENGTH[1]);

    final int termSeedSize = (int) ((fieldsCount * documentsCount
            * MIN_MAX_DOC_LENGTH[1]) * 0.0005);

    // generate a seed of random terms
    LOG.info("Creating term seed with {} terms.", termSeedSize);
    final List<String> seedTermList = new ArrayList<>(termSeedSize);
    while (seedTermList.size() < termSeedSize) {
      seedTermList.add(RandomValue.getString(MIN_MAX_TERM_LENGTH[0],
              MIN_MAX_TERM_LENGTH[1]));
    }

    // gather some documents for bulk commits
    final double bulkSize = documentsCount * 0.1;
    List<String[]> documentList = new ArrayList<>((int) bulkSize);

    LOG.info("Creating {} documents.", documentsCount);
    // create documents
    for (int doc = 0; doc < documentsCount; doc++) {
      final String[] docContent = new String[fieldsCount];

      // create document fields
      for (int field = 0; field < fieldsCount; field++) {
        int fieldTerms = RandomValue.getInteger(MIN_MAX_DOC_LENGTH[0],
                MIN_MAX_DOC_LENGTH[1]);
        StringBuilder content = new StringBuilder(fieldTerms
                * MIN_MAX_TERM_LENGTH[1]);
        Map<BytesWrap, AtomicInteger> fieldTermFreq = new HashMap(fieldTerms);

        // create terms for each field
        for (int term = 0; term < fieldTerms; term++) {
          // pick a document term from the seed
          final String docTerm = seedTermList.get(RandomValue.getInteger(0,
                  seedTermList.size() - 1));
          // add it to the list of known terms
          final BytesWrap docTermBw = new BytesWrap(docTerm.getBytes("UTF-8"));

          // count term frequencies
          if (fieldTermFreq.containsKey(docTermBw)) {
            fieldTermFreq.get(docTermBw).incrementAndGet();
          } else {
            fieldTermFreq.put(docTermBw.clone(), new AtomicInteger(1));
          }

          // append term to content
          content.append(docTerm);
          if (term + 1 < fieldTerms) {
            content.append(' ');
          }
        }
        // store document field
        docContent[field] = content.toString();

        // store document field term frequency value
        for (Entry<BytesWrap, AtomicInteger> ftfEntry : fieldTermFreq.
                entrySet()) {
          idx.put(Fun.t3(field, doc, ftfEntry.getKey().clone()), ftfEntry.
                  getValue().longValue());
        }
      }

      // commit documents in bulk to index
      if (documentList.size() > bulkSize) {
        tmpIdx.addDocs(documentList);
        documentList = new ArrayList<>((int) bulkSize);
      }

      // store document content to bulk commit cache
      documentList.add(docContent.clone());
    }
    // comit any leftover documents
    if (!documentList.isEmpty()) {
      tmpIdx.addDocs(documentList);
    }
    tmpIdx.flush();

    initialized = true;
  }

  /**
   * Get a random query matching terms from the documents in index.
   *
   * @return Random query
   * @throws ParseException Thrown on query parsing errors
   */
  public String getQueryString() throws ParseException {
    return getQueryString(null);
  }

  /**
   * Create a query object from the given terms.
   *
   * @param queryTerms Terms to use in query
   * @return A query object consisting of the given terms
   * @throws ParseException Thrown on query parsing errors
   */
  public String getQueryString(final String[] queryTerms) throws
          ParseException {
    final String queryStr;

    if (queryTerms != null) {
      // create query string from passed-in terms
      queryStr = StringUtils.join(queryTerms, " ");
    } else {
      // create a random query string
      if (getTermSet().isEmpty()) {
        throw new IllegalStateException("No terms in index.");
      }

      final List<BytesWrap> idxTerms = new ArrayList<>(getTermSet());
      final int queryLength = RandomValue.getInteger(MIN_MAX_QUERY_LENGTH[0],
              MIN_MAX_QUERY_LENGTH[1]);
      final Collection<String> terms = new HashSet<>(queryLength);

      for (int i = 0; i < queryLength;) {
        if (terms.add(idxTerms.get(RandomValue.
                getInteger(0, idxTerms.size() - 1)).toString())) {
          i++;
        }
      }

      queryStr = StringUtils.join(terms.toArray(new String[terms.
              size()]), " ");
    }

    LOG.debug("Test query: {}", queryStr);
    return queryStr;
  }

  /**
   * Get the reader to the {@link TempDiskIndex}.
   *
   * @return Index reader
   * @throws IOException Thrown on low-level I/O errors
   */
  public IndexReader getReader() throws IOException {
    return tmpIdx.getReader();
  }

  /**
   * Get the bit-mask of active index fields.
   *
   * @return bit-mask of active index fields. 0 means off, 1 means on
   */
  public int[] getFieldState() {
    return activeFieldState.clone();
  }

  /**
   * Set index field bit-mask.
   *
   * @param state bit-mask of active index fields. 0 means off, 1 means on
   */
  public void setFieldState(final int[] state) {
    for (int i = 0; i < state.length && i < activeFieldState.length; i++) {
      activeFieldState[i] = state[i];
    }
  }

  /**
   * Set the active state of a field.
   *
   * @param fieldName Name of the field
   * @param state True, to use this field for calculation
   * @return Old state value, or <tt>null</tt> if the field does not exist
   */
  public Boolean setFieldState(final String fieldName, final boolean state) {
    Boolean oldState = null;
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).equals(fieldName)) {
        oldState = activeFieldState[i] == 1;
        if (state) {
          activeFieldState[i] = 1;
        } else {
          activeFieldState[i] = 0;
        }
      }
    }
    return oldState;
  }

  /**
   * Get a collection of all known document-ids.
   *
   * @return All known document-ids
   */
  private Collection<Integer> getDocumentIds() {
    final Collection<Integer> docIds = new ArrayList(documentsCount);
    for (int i = 0; i < documentsCount; i++) {
      docIds.add(i);
    }
    return docIds;
  }

  /**
   * Get a map with <tt>term -> document-frequency</tt> values for a specific
   * document.
   *
   * @param docId Document to lookup
   * @return Map with <tt>term -> document-frequency</tt> values
   */
  public Map<BytesWrap, Number> getDocumentTermFrequencyMap(final int docId) {
    if (docId < 0 || docId > documentsCount) {
      throw new IllegalArgumentException("Illegal document id " + docId + ".");
    }

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<BytesWrap, Number> termFreqMap = new HashMap<>();

    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (activeFieldState[fieldNum] == 1) {
        Iterable<BytesWrap> docTerms = Fun.filter(idx.keySet(), Integer.
                valueOf(fieldNum), docId);
        for (BytesWrap term : docTerms) {
          final Long docTermFreq = idx.get(Fun.t3(
                  fieldNum, docId, term));
          if (termFreqMap.containsKey(term)) {
            termFreqMap.put(term, termFreqMap.get(term).
                    longValue() + docTermFreq);
          } else {
            termFreqMap.put(term, docTermFreq);
          }
        }
      }
    }
    return termFreqMap;
  }

  /**
   * Get a set of unique terms for a document.
   *
   * @param docId Document to lookup
   * @return List of unique terms in document
   */
  public Collection<BytesWrap> getDocumentTermSet(final int docId) {
    checkDocumentId(docId);
    return getDocumentTermFrequencyMap(docId).keySet();
  }

  /**
   * Checks, if a document-id is valid (in index).
   *
   * @param docId Document-id to check
   */
  private void checkDocumentId(final int docId) {
    if (docId < 0 || docId > documentsCount - 1) {
      throw new IllegalArgumentException("Illegal document id: " + docId);
    }
  }

  /**
   * Get the overall term-frequency for a specific document.
   *
   * @param docId Document to lookup
   * @return overall term-frequency
   */
  private int getDocumentTermFrequency(final int docId) {
    final Map<BytesWrap, Number> docTermMap = getDocumentTermFrequencyMap(
            docId);
    int docTermCount = 0;
    for (Number count : docTermMap.values()) {
      docTermCount += count.intValue();
    }
    return docTermCount;
  }

  /**
   * Get a list of all index terms. The list is <u>not</u> unique.
   *
   * @return List of all terms in index
   */
  public Collection<BytesWrap> getTermList() {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<BytesWrap> terms = new ArrayList<>();
    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < documentsCount; docId++) {
          Iterable<BytesWrap> docTerms = Fun.filter(idx.keySet(), Integer.
                  valueOf(fieldNum), docId);
          for (BytesWrap docTerm : docTerms) {
            final long docTermFreqCount = idx.get(Fun.t3(fieldNum,
                    docId, docTerm));
            for (int docTermFreq = 0; docTermFreq < docTermFreqCount;
                    docTermFreq++) {
              terms.add(docTerm);
            }
          }
        }
      }
    }
    return terms;
  }

  /**
   * Get a unique set of all index terms.
   *
   * @return Set of all terms in index
   */
  public Collection<BytesWrap> getTermSet() {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<BytesWrap> terms = new HashSet<>();

    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < documentsCount; docId++) {
          Iterable<BytesWrap> docTerms = Fun.filter(idx.keySet(), fieldNum,
                  docId);
          for (BytesWrap docTerm : docTerms) {
            if (docTerm == null) {
              throw new IllegalStateException("Terms was null. doc=" + docId
                      + " field=" + fields.get(fieldNum));
            }
            terms.add(docTerm);
          }
        }
      }
    }
    return terms;
  }

  @Override
  @SuppressWarnings("CollectionWithoutInitialCapacity")
  public void clearTermData() {
    this.prefixMap = new ConcurrentHashMap<>();
  }

  @Override
  public long getTermFrequency() {
    Long frequency = 0L;

    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      int tf = 0;
      if (activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < documentsCount; docId++) {
          Iterable<BytesWrap> docTerms = Fun.filter(idx.keySet(), Integer.
                  valueOf(fieldNum), docId);
          for (BytesWrap docTerm : docTerms) {
            frequency += idx.get(Fun.t3(fieldNum, docId, docTerm));
            tf += idx.get(Fun.t3(fieldNum, docId, docTerm));
          }
        }
      }
    }
    return frequency;
  }

  @Override
  public Long getTermFrequency(final BytesWrap term) {
    Long frequency = 0L;

    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < documentsCount; docId++) {
          final Long docTermFreq = idx.get(Fun.t3(fieldNum,
                  docId, term));
          if (docTermFreq != null) {
            frequency += docTermFreq;
          }
        }
      }
    }
    return frequency == 0 ? null : frequency;
  }

  @Override
  public double getRelativeTermFrequency(final BytesWrap term) {
    Long termFrequency = getTermFrequency(term);
    if (termFrequency == null) {
      return 0;
    }
    return termFrequency.doubleValue() / Long.valueOf(getTermFrequency()).
            doubleValue();
  }

  @Override
  public String[] getFields() {
    List<String> fieldNames = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      if (activeFieldState[i] == 1) {
        fieldNames.add(fields.get(i));
      }
    }

    return fieldNames.toArray(new String[fieldNames.size()]);
  }

  @Override
  public Iterator<BytesWrap> getTermsIterator() {
    return getTermSet().iterator();
  }

  @Override
  public Source<BytesWrap> getTermsSource() {
    return new CollectionSource<>(getTermSet());
  }

  @Override
  public Iterator<Integer> getDocumentIdIterator() {
    return getDocumentIds().iterator();
  }

  @Override
  public Source<Integer> getDocumentIdSource() {
    return new CollectionSource<>(getDocumentIds());
  }

  @Override
  public long getUniqueTermsCount() {
    return getTermSet().size();
  }

  @Override
  @SuppressWarnings({"SynchronizeOnNonFinalField",
    "CollectionWithoutInitialCapacity"})
  public Object setTermData(final String prefix, final int documentId,
          final BytesWrap term, final String key, final Object value) {
    checkDocumentId(documentId);

    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key was empty or null.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Value was null.");
    }

    synchronized (this.prefixMap) {
      Map<Tuple.Tuple3<Integer, BytesWrap, String>, Object> dataMap;
      if (this.prefixMap.containsKey(prefix)) {
        dataMap = this.prefixMap.get(prefix);
      } else {
        dataMap = new ConcurrentHashMap<>();
        this.prefixMap.put(prefix, dataMap);
      }

      return dataMap.put(Tuple.tuple3(documentId, term, key), value);
    }
  }

  @Override
  public Object getTermData(final String prefix, final int documentId,
          final BytesWrap term, final String key) {
    checkDocumentId(documentId);
    Map<Tuple.Tuple3<Integer, BytesWrap, String>, Object> dataMap
            = this.prefixMap.get(prefix);
    if (dataMap == null) {
      return null;
    }
    return dataMap.get(Tuple.tuple3(documentId, term, key));
  }

  @Override
  public Map<BytesWrap, Object> getTermData(final String prefix,
          final int documentId, final String key) {
    checkDocumentId(documentId);
    Map<Tuple.Tuple3<Integer, BytesWrap, String>, Object> dataMap
            = this.prefixMap.get(prefix);
    if (dataMap == null) {
      return null;
    }
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<BytesWrap, Object> returnMap = new HashMap<>();
    for (Entry<Tuple.Tuple3<Integer, BytesWrap, String>, Object> dataEntry
            : dataMap.entrySet()) {
      if (dataEntry.getKey().a.equals(documentId) && dataEntry.getKey().c.
              equals(key)) {
        returnMap.put(dataEntry.getKey().b, dataEntry.getValue());
      }
    }
    return returnMap;
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocumentId(docId);
    final int docTermFreq = getDocumentTermFrequency(docId);
    final DocumentModel.DocumentModelBuilder dmBuilder
            = new DocumentModel.DocumentModelBuilder(docId, docTermFreq);
    dmBuilder.setTermFrequency(getDocumentTermFrequencyMap(docId));
    return dmBuilder.getModel();
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    return !(docId < 0 || docId > (documentsCount - 1));
  }

  @Override
  public long getDocumentCount() {
    return documentsCount;
  }

  @Override
  public boolean documentContains(final int documentId, final BytesWrap term) {
    checkDocumentId(documentId);
    return getDocumentTermFrequencyMap(documentId).keySet().contains(term);
  }

  @Override
  public String getProperty(final String prefix, final String key,
          final String defaultValue) {
    return defaultValue;
  }

  @Override
  public void setProperty(final String prefix, final String key,
          final String value) {
    // nothing to do here
  }

  @Override
  public void dispose() {
    // NOP
  }

  @Override
  public Collection<BytesWrap> getDocumentsTermSet(final Collection<Integer> docIds) {
    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<BytesWrap> terms = new HashSet<>();

    for (Integer docId : uniqueDocIds) {
      terms.addAll(getDocumentTermSet(docId));
    }
    return terms;
  }

  // ---------- TEST ACCESSORS ----------
  /**
   * Get the known fields list.
   *
   * @return Fields list
   */
  protected static List<String> test_getFields() {
    return Collections.unmodifiableList(fields);
  }

  /**
   * Get the bit-flags for each field.
   *
   * @return Field flags
   */
  protected int[] test_getActiveFieldState() {
    return activeFieldState.clone();
  }

  /**
   * Get the initialized state.
   *
   * @return True, if initialized
   */
  public static boolean test_isInitialized() {
    return initialized;
  }

  // ---------- UNSUPPORTED OPERATIONS ----------
  @Override
  public void clearProperties() {
    // nothing here
  }

  @Override
  public boolean addDocument(final DocumentModel docModel) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getProperty(final String prefix, final String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
