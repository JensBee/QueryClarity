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

import de.unihildesheim.ByteArray;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.query.SimpleTermsQuery;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.lucene.util.TempDiskIndex;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.StringUtils;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Temporary index of random generated documents for testing purposes.
 *
 *
 */
public final class TestIndexDataProvider implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          TestIndexDataProvider.class);

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
   * List of stop-words to exclude from term frequency calculations.
   */
  private static Collection<ByteArray> stopWords = Collections.
          <ByteArray>emptySet();

  @Override
  public void loadCache(String name) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void loadOrCreateCache(String name) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void createCache(String name) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Configuration to create different sizes of test indexes.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum IndexSize {

    /**
     * Configuration for a small sized test index.
     */
    SMALL(new int[]{
      // min/max documents
      10, 100,
      // min/max fields
      1, 5,
      // min/max document length (terms)
      10, 150,
      // min/max term length
      1, 10,
      // min/max query length (terms)
      1, 5
    }),
    /**
     * Configuration for a medium sized test index.
     */
    MEDIUM(new int[]{
      // min/max documents
      500, 2500,
      // min/max fields
      1, 10,
      // min/max document length (terms)
      250, 1500,
      // min/max term length
      1, 20,
      // min/max query length (terms)
      1, 10
    }),
    /**
     * Configuration for a large sized test index.
     */
    LARGE(new int[]{
      // min/max documents
      100, 10000,
      // min/max fields
      1, 15,
      // min/max document length (terms)
      250, 5000,
      // min/max term length
      1, 20,
      // min/max query length (terms)
      1, 15
    });

    /**
     * Minimum and maximum amount of fields to create.
     */
    final int[] fieldCount;
    /**
     * Minimum and maximum number of documents to create.
     */
    final int[] docCount;
    /**
     * Minimum and maximum length of a random generated document (in terms per
     * field).
     */
    final int[] docLength;
    /**
     * Minimum and maximum length of a random term in a document.
     */
    final int[] termLength;
    /**
     * Minimum and maximum length of a random generated query.
     */
    final int[] queryLength;

    /**
     * Initialize the index configuration.
     *
     * @param sizes Size configuration
     */
    IndexSize(final int[] sizes) {
      docCount = new int[]{sizes[0], sizes[1]};
      fieldCount = new int[]{sizes[2], sizes[3]};
      docLength = new int[]{sizes[4], sizes[5]};
      termLength = new int[]{sizes[6], sizes[7]};
      queryLength = new int[]{sizes[8], sizes[9]};
    }
  }

  /**
   * Size of the index actually used.
   */
  protected IndexSize idxConf;

  /**
   * Number of documents in index.
   */
  private static int documentsCount;

  /**
   * Field, Document-id, Term -> Frequency.
   */
  private static ConcurrentNavigableMap<
          Fun.Tuple3<Integer, Integer, ByteArray>, Long> idx;

  /**
   * Create a new test index with a specific size constraint.
   *
   * @param indexSize Size
   * @throws IOException Thrown on low-level I/O errors
   */
  @SuppressWarnings({"LeakingThisInConstructor",
    "CollectionWithoutInitialCapacity"})
  public TestIndexDataProvider(final IndexSize indexSize) throws IOException {
    if (!initialized) {
      idxConf = indexSize;
      createIndex();
    }
    this.activeFieldState = new int[fields.size()];
    // set all fields active
    Arrays.fill(this.activeFieldState, 1);
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    if (stopWords.contains(term)) {
      return 0;
    }
    int freq = 0;
    for (Integer docId : getDocumentIds()) {
      if (documentContains(docId, term)) {
        freq++;
      }
    }
    return freq;
  }

  /**
   * Initialize the testing index with default values.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  @SuppressWarnings("CollectionWithoutInitialCapacity")
  public TestIndexDataProvider() throws IOException {
    this(IndexSize.MEDIUM);
  }

  /**
   * Create a simple test index and initialize the {@link TempDiskIndex}.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  private void createIndex() throws IOException {
    idx = DBMaker.newTempFileDB()
            .transactionDisable()
            .asyncWriteEnable()
            .asyncWriteFlushDelay(100)
            .mmapFileEnableIfSupported()
            .make().createTreeMap("idx").keySerializer(
                    BTreeKeySerializer.TUPLE3).valueSerializer(
                    Serializer.LONG).make();

    // generate random document fields
    final int fieldsCount = RandomValue.getInteger(idxConf.fieldCount[0],
            idxConf.fieldCount[1]);
    fields = new ArrayList<>(fieldsCount);
    for (int i = 0; i < fieldsCount; i++) {
      fields.add(i + "_" + RandomValue.getString(3, 10));
    }

    // create the lucene index
    tmpIdx = new TempDiskIndex(fields.toArray(new String[fields.size()]));

    // set the number of random documents to create
    documentsCount = RandomValue.getInteger(idxConf.docCount[0],
            idxConf.docCount[1]);

    LOG.info("Creating a {} sized index with {} documents, {} fields each "
            + "and a maximum of {} terms per field. This may take some time.",
            idxConf.toString(), documentsCount, fieldsCount,
            idxConf.docLength[1]);

    final int termSeedSize = (int) ((fieldsCount * documentsCount
            * idxConf.docLength[1]) * 0.005);

    // generate a seed of random terms
    LOG.info("Creating term seed with {} terms.", termSeedSize);
    final List<String> seedTermList = new ArrayList<>(termSeedSize);
    while (seedTermList.size() < termSeedSize) {
      seedTermList.add(RandomValue.getString(idxConf.termLength[0],
              idxConf.termLength[1]));
    }

    LOG.info("Creating {} documents.", documentsCount);
    final Collection<Integer> latch = new ArrayList<>(documentsCount);
    for (int doc = 0; doc < documentsCount; doc++) {
      latch.add(doc);
    }
    Map<Integer, String[]> idxMap = DBMaker.newTempTreeMap();
    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(latch),
                    new DocCreator(idxMap, seedTermList, fieldsCount)
            )).process();
    for (String[] c : idxMap.values()) {
      TestIndexDataProvider.tmpIdx.addDoc(c);
    }
    tmpIdx.flush();
    idxMap.clear();

    initialized = true;
  }

  /**
   * Get the Lucene index.
   *
   * @return Lucene index
   */
  public TempDiskIndex getIndex() {
    return tmpIdx;
  }

  /**
   * Get the file directory where the temporary index resides in.
   *
   * @return Index directory path as string
   */
  public String getIndexDir() {
    return tmpIdx.getIndexDir();
  }

  /**
   * Get a random query matching terms from the documents in index. The terms
   * in the query are not unique. The query string may contain stopwords.
   *
   * @return Random query string
   * @throws ParseException Thrown on query parsing errors
   */
  public String getQueryString() throws ParseException {
    return getQueryString(null, false);
  }

  /**
   * Get a random query matching terms from the documents in index. The terms
   * in the query are not unique. The query string may contain stopwords.
   *
   * @return Query generated from random query string
   * @throws ParseException Thrown on query parsing errors
   */
  public SimpleTermsQuery getQueryObj() throws ParseException {
    return TermsQueryBuilder.buildFromEnvironment(getQueryString());
  }

  /**
   * Get a random query matching terms from the documents in index. All terms
   * in the query are unique. The query string may contain stopwords.
   *
   * @return Random query String
   * @throws ParseException Thrown on query parsing errors
   */
  public String getUniqueQueryString() throws ParseException {
    return getQueryString(null, true);
  }

  /**
   * Create a query string from the given terms. The query string may contain
   * stopwords.
   *
   * @param queryTerms Terms to use in query
   * @return A query String consisting of the given terms
   * @throws ParseException Thrown on query parsing errors
   */
  public String getQueryString(final String[] queryTerms) throws
          ParseException {
    return getQueryString(queryTerms, false);
  }

  /**
   * Create a query object from the given terms. The query string may contain
   * stopwords.
   *
   * @param queryTerms Terms to use in query
   * @return A query object consisting of the given terms
   * @throws ParseException Thrown on query parsing errors
   */
  public SimpleTermsQuery getQueryObj(final String[] queryTerms) throws
          ParseException {
    return TermsQueryBuilder.buildFromEnvironment(getQueryString(queryTerms));
  }

  /**
   * Create a query object from the given terms or create a random query, if
   * no terms were given. The query string may contain stopwords.
   *
   * @param queryTerms List of terms to include in the query, or null to
   * create a random query
   * @param unique If true and not terms are given, then query terms are
   * unique
   * @return A query String consisting of the given terms
   * @throws ParseException Thrown on query parsing errors
   */
  private String getQueryString(final String[] queryTerms,
          final boolean unique) throws ParseException {
    final String queryStr;

    if (queryTerms != null) {
      if (queryTerms.length == 0) {
        throw new IllegalArgumentException("Query terms where empty.");
      }
      // create query string from passed-in terms
      queryStr = StringUtils.join(queryTerms, " ");
    } else {
      // create a random query string
      if (getTermSet().isEmpty()) {
        throw new IllegalStateException("No terms in index.");
      }

      final List<ByteArray> idxTerms = new ArrayList<>(getTermSet());
      final int queryLength = RandomValue.getInteger(idxConf.queryLength[0],
              idxConf.queryLength[1]);

      final Collection<String> terms;

      // check, if terms should be unique
      if (unique) {
        terms = new HashSet<>(queryLength);
      } else {
        terms = new ArrayList<>(queryLength);
      }

      if (idxConf.queryLength[0] >= idxTerms.size()) {
        LOG.warn("Minimum query length exceedes unique term count in index. "
                + "Adding all index terms to query.");
        for (ByteArray term : idxTerms) {
          terms.add(ByteArrayUtil.utf8ToString(term));
        }
      } else if (queryLength >= idxTerms.size()) {
        LOG.warn("Random query length exceedes unique term count in index. "
                + "Adding all index terms to query.");
        for (ByteArray term : idxTerms) {
          terms.add(ByteArrayUtil.utf8ToString(term));
        }
      } else {
        for (int i = 0; i < queryLength;) {
          if (terms.add(ByteArrayUtil.utf8ToString(idxTerms.get(RandomValue.
                  getInteger(0, idxTerms.size() - 1))))) {
            i++;
          }
        }
      }

      queryStr = StringUtils.join(terms.toArray(new String[terms.
              size()]), " ");
    }

    LOG.debug("Test query: {}", queryStr);
    return queryStr;
  }

  /**
   * Get a random subset of all available fields.
   *
   * @return Collection of random fields
   */
  public Collection<String> getRandomFields() {
    Collection<String> fieldNames;
    if (fields.size() == 1) {
      fieldNames = new ArrayList<>(1);
      fieldNames.add(fields.get(0));
    } else {
      final int[] newFieldState = this.activeFieldState.clone();
      // ensure both states are not the same
      while (Arrays.equals(newFieldState, this.activeFieldState)) {
        for (int i = 0; i < this.activeFieldState.length; i++) {
          newFieldState[i] = RandomValue.getInteger(0, 1);
        }
      }

      fieldNames = new ArrayList<>(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        if (newFieldState[i] == 1) {
          fieldNames.add(fields.get(i));
        }
      }

      // lazy backup - add a random field, if none set already
      if (fieldNames.isEmpty()) {
        fieldNames.add(fields.
                get(RandomValue.getInteger(0, fields.size() - 1)));
      }
    }
    return fieldNames;
  }

  /**
   * Set all fields active.
   */
  private void enableAllFields() {
    Arrays.fill(this.activeFieldState, 1);
    LOG.debug("Enabled all {} fields.", this.activeFieldState.length);
  }

  /**
   * Prepare the {@link Environment} for testing.
   *
   * @param newFields Document fields to use
   * @param newStopwords List of stopwords to set
   * @return {@link Environment} instance
   * @throws IOException Thrown on low-level i/o errors
   */
  private Environment.Builder prepareEnvironment(
          final Collection<String> newFields,
          final Collection<String> newStopwords) throws IOException {
    final File dataDir = new File(getIndexDir() + File.separatorChar + "data");
    if (!dataDir.exists() && !dataDir.mkdirs()) {
      throw new IOException("Failed to create data directory: '" + dataDir
              + "'");
    }

    final String[] activeFields;
    if (newFields == null) {
      enableAllFields();
      activeFields = getActiveFieldNames().toArray(
              new String[this.activeFieldState.length]);
    } else {
      activeFields = newFields.toArray(new String[newFields.size()]);
      Arrays.fill(this.activeFieldState, 0);
      // activate single fields
      for (String field : newFields) {
        setFieldState(field, true);
      }
    }

    if (newStopwords == null) {
      TestIndexDataProvider.stopWords = Collections.emptySet();
    } else {
      TestIndexDataProvider.stopWords = DBMaker.newTempHashSet();
      for (String stopWord : newStopwords) {
        try {
          TestIndexDataProvider.stopWords.add(new ByteArray(stopWord.getBytes(
                  "UTF-8")));
        } catch (UnsupportedEncodingException ex) {
          LOG.error("Error adding stopword '" + stopWord + "'.", ex);
        }
      }
    }

    LOG.info("Preparing Environment. fields={}", Arrays.toString(
            activeFields));

    final Environment.Builder envBuilder = new Environment.Builder(
            getIndexDir(), dataDir.getPath())
            .fields(activeFields)
            .testRun();
    if (newStopwords != null) {
      LOG.info("Preparing Environment. stopwords={}", newStopwords);
      envBuilder.stopwords(newStopwords);
    }

    return envBuilder;
  }

  /**
   * Setup the {@link Environment} based around the test index.
   *
   * @param dataProv DataProvider to use. May be null, to get a default one.
   * @param fields Document fields to use
   * @param stopwords List of stopwords to set
   * @throws Exception Thrown on low-level I/O errors or if the desired
   * DataProvider could not be created
   */
  public void setupEnvironment(
          final Class<? extends IndexDataProvider> dataProv,
          final Collection<String> fields,
          final Collection<String> stopwords) throws
          Exception {
    prepareEnvironment(fields, stopwords).dataProvider(dataProv).build();
  }

  /**
   * Setup the {@link Environment} using the given {@link IndexDataProvider}
   * instance.
   *
   * @param dataProv DataProvider to use.
   * @throws Exception Thrown on low-level I/O errors or if the desired
   * DataProvider could not be created
   */
  public void setupEnvironment(final IndexDataProvider dataProv,
          final Collection<String> fields,
          final Collection<String> stopwords) throws
          Exception {
    prepareEnvironment(fields, stopwords).dataProvider(dataProv).build();
  }

  /**
   * Setup the {@link Environment} based around the test index using a default
   * {@link IndexDataProvider} implementation.
   *
   * @throws Exception Thrown on low-level I/O errors or if the desired
   * DataProvider could not be created
   */
  public void setupEnvironment(final Collection<String> fields,
          final Collection<String> stopwords) throws Exception {
    prepareEnvironment(fields, stopwords).dataProvider(this).build();
  }

  /**
   * Get a list of active fields.
   *
   * @return Collection of active fields
   */
  public Collection<String> getActiveFieldNames() {
    Collection<String> fieldNames = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      if (activeFieldState[i] == 1) {
        fieldNames.add(fields.get(i));
      }
    }
    return fieldNames;
  }

  /**
   * Set the active state of a field.
   *
   * @param fieldName Name of the field
   * @param state True, to use this field for calculation
   * @return Old state value, or <tt>null</tt> if the field does not exist
   */
  private Boolean setFieldState(final String fieldName, final boolean state) {
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
    final Collection<Integer> docIds = new ArrayList<>(documentsCount);
    for (int i = 0; i < documentsCount; i++) {
      docIds.add(i);
    }
    return docIds;
  }

  /**
   * Get a map with <tt>term -> frequency (in document)</tt> values for a
   * specific document.
   *
   * @param docId Document to lookup
   * @return Map with <tt>term -> in-document-frequency</tt> values
   */
  public Map<ByteArray, Long> getDocumentTermFrequencyMap(final int docId) {
    if (docId < 0 || docId > documentsCount) {
      throw new IllegalArgumentException("Illegal document id " + docId + ".");
    }

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<ByteArray, Long> termFreqMap = new HashMap<>();

    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (activeFieldState[fieldNum] == 1) {
        Iterable<ByteArray> docTerms = Fun.filter(idx.keySet(), fieldNum,
                docId);
        for (ByteArray term : docTerms) {
          if (stopWords.contains(term)) { // skip stopwords
            continue;
          }
          final Long docTermFreq = idx.get(Fun.t3(fieldNum, docId, term));
          if (termFreqMap.containsKey(term)) {
            termFreqMap.put(term.clone(), termFreqMap.get(term) + docTermFreq);
          } else {
            termFreqMap.put(term.clone(), docTermFreq);
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
  public Collection<ByteArray> getDocumentTermSet(final int docId) {
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
    final Map<ByteArray, Long> docTermMap = getDocumentTermFrequencyMap(
            docId);
    int docTermCount = 0;
    for (Number count : docTermMap.values()) {
      docTermCount += count.intValue();
    }
    return docTermCount;
  }

  /**
   * Get a list of all index terms. The list is <u>not</u> unique. Stopwords
   * will be excluded.
   *
   * @return List of all terms in index
   */
  public Collection<ByteArray> getTermList() {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new ArrayList<>();
    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < documentsCount; docId++) {
          Iterable<ByteArray> docTerms = Fun.filter(idx.keySet(), fieldNum,
                  docId);
          for (ByteArray docTerm : docTerms) {
            if (stopWords.contains(docTerm)) { // skip stopwords
              continue;
            }
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
  public Collection<ByteArray> getTermSet() {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new HashSet<>();

    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < documentsCount; docId++) {
          Iterable<ByteArray> docTerms = Fun.filter(idx.keySet(), fieldNum,
                  docId);
          for (ByteArray docTerm : docTerms) {
            if (stopWords.contains(docTerm)) { // skip stopwords
              continue;
            }
            if (docTerm == null) {
              throw new IllegalStateException("Terms was null. doc=" + docId
                      + " field=" + fields.get(fieldNum));
            }
            terms.add(docTerm.clone());
          }
        }
      }
    }
    return terms;
  }

  @Override
  public long getTermFrequency() {
    Long frequency = 0L;

    for (int fieldNum = 0; fieldNum < fields.size(); fieldNum++) {
      if (this.activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < TestIndexDataProvider.documentsCount;
                docId++) {
          Iterable<ByteArray> docTerms = Fun.filter(TestIndexDataProvider.idx.
                  keySet(), fieldNum, docId);
          for (ByteArray docTerm : docTerms) {
            if (TestIndexDataProvider.stopWords.contains(docTerm)) {
              // skip stopwords
              continue;
            }
            frequency += TestIndexDataProvider.idx.get(Fun.t3(fieldNum, docId,
                    docTerm));
          }
        }
      }
    }
    return frequency;
  }

  @Override
  public Long getTermFrequency(final ByteArray term) {
    Long frequency = 0L;
    if (stopWords.contains(term)) { // skip stopwords
      return frequency;
    }

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
  public double getRelativeTermFrequency(final ByteArray term) {
    if (stopWords.contains(term)) { // skip stopwords
      return 0d;
    }

    Long termFrequency = getTermFrequency(term);
    if (termFrequency == null) {
      return 0;
    }
    return termFrequency.doubleValue() / Long.valueOf(getTermFrequency()).
            doubleValue();
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    return getTermSet().iterator();
  }

  @Override
  public Source<ByteArray> getTermsSource() {
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
  public boolean documentContains(final int documentId, final ByteArray term) {
    if (stopWords.contains(term)) { // skip stopwords
      return false;
    }
    checkDocumentId(documentId);
    return getDocumentTermFrequencyMap(documentId).keySet().contains(
            term);
  }

  @Override
  public void dispose() {
    // NOP
  }

  @Override
  public Collection<ByteArray> getDocumentsTermSet(
          final Collection<Integer> docIds) {
    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new HashSet<>();

    for (Integer docId : uniqueDocIds) {
      terms.addAll(getDocumentTermSet(docId));
    }
    return terms;
  }

  @Override
  public void warmUp() {
    // NOP
  }

  /**
   * Get the initialized state.
   *
   * @return True, if initialized
   */
  public static boolean isInitialized() {
    return initialized;
  }

  /**
   * {@link Processing} {@link Target} creating random documents.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class DocCreator extends Target.TargetFunc<Integer> {

    /**
     * Seed list with document terms.
     */
    private final List<String> seedTermList;
    /**
     * Number of fields to create.
     */
    private final int fieldsCount;
    /**
     * Map caching target documents.
     */
    private Map<Integer, String[]> contentCache;

    /**
     * Factory instance creator
     *
     * @param targetMap Target map to put results in
     * @param newSeedTermList List with terms to add to documents
     * @param newFieldsCount Number of fields to create
     */
    private DocCreator(
            final Map<Integer, String[]> targetMap,
            final List<String> newSeedTermList,
            final int newFieldsCount) {
      this.seedTermList = newSeedTermList;
      this.fieldsCount = newFieldsCount;
      this.contentCache = targetMap;
    }

    @Override
    public void call(final Integer docId) {
      if (docId == null) {
        return;
      }

      final String[] docContent = new String[fieldsCount];

      // create document fields
      for (int field = 0; field < fieldsCount; field++) {
        int fieldTerms = RandomValue.getInteger(idxConf.docLength[0],
                idxConf.docLength[1]);
        final StringBuilder content = new StringBuilder(fieldTerms
                * idxConf.termLength[1]);
        final Map<ByteArray, AtomicInteger> fieldTermFreq
                = new HashMap<>(fieldTerms);

        // create terms for each field
        for (int term = 0; term < fieldTerms; term++) {
          // pick a document term from the seed
          final String docTerm = seedTermList.get(RandomValue.getInteger(0,
                  seedTermList.size() - 1));
          // add it to the list of known terms
          final ByteArray docTermBytes;
          try {
            docTermBytes = new ByteArray(docTerm.getBytes("UTF-8"));
          } catch (UnsupportedEncodingException ex) {
            LOG.error("Error encoding term. term={}", docTerm);
            continue;
          }

          // count term frequencies
          if (fieldTermFreq.containsKey(docTermBytes)) {
            fieldTermFreq.get(docTermBytes).incrementAndGet();
          } else {
            fieldTermFreq.put(docTermBytes,
                    new AtomicInteger(1));
          }

          // append term to content
          content.append(docTerm).append(' ');
        }
        // store document field
        docContent[field] = content.toString().trim();
        LOG.debug("DOC doc={} f={} c={}", docId, field, docContent[field]);

        // store document field term frequency value
        for (Entry<ByteArray, AtomicInteger> ftfEntry : fieldTermFreq.
                entrySet()) {
          idx.put(Fun.t3(field, docId, ftfEntry.getKey()),
                  ftfEntry.getValue().longValue());
        }
      }

      this.contentCache.put(docId, docContent);
    }
  }
}
