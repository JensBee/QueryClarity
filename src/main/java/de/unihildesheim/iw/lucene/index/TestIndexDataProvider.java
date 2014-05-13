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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.query.SimpleTermsQuery;
import de.unihildesheim.iw.lucene.query.TermsQueryBuilder;
import de.unihildesheim.iw.lucene.util.TempDiskIndex;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Temporary index of random generated documents for testing purposes.
 *
 * @author Jens Bertram
 */
public final class TestIndexDataProvider
    implements IndexDataProvider {

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
   * Bit-mask storing active fields state. 0 means off 1 on. Index is related to
   * {@link #fields}.
   */
  private static int[] activeFieldState;

  /**
   * Flag indicating, if all static fields have been initialized.
   */
  private static boolean initialized = false;

  /**
   * Temporary Lucene index held in memory.
   */
  static TempDiskIndex tmpIdx;

  /**
   * List of stop-words to exclude from term frequency calculations.
   */
  private static Collection<ByteArray> stopWords = Collections.emptySet();

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
   * Default index size to use for running tests.
   */
  public static final IndexSize DEFAULT_INDEX_SIZE = IndexSize.SMALL;

  /**
   * Size of the index actually used.
   */
  private static IndexSize idxConf;

  /**
   * Number of documents in index.
   */
  private static int documentsCount;

  /**
   * Field, Document-id, Term -> Frequency.
   */
  private static ConcurrentNavigableMap<
      Fun.Tuple3<Integer, Integer, ByteArray>, Long> idx;

  public static Reference reference;
  public static Util util;

  /**
   * Create a new test index with a specific size constraint.
   *
   * @param indexSize Size
   * @throws IOException Thrown on low-level I/O errors
   */
  public TestIndexDataProvider(final IndexSize indexSize)
      throws IOException, ProcessingException {
    if (indexSize == null) {
      throw new IllegalArgumentException("Index size was null.");
    }
    if (!TestIndexDataProvider.initialized) {
      reference = new Reference();
      util = new Util();
      TestIndexDataProvider.idxConf = indexSize;
      createIndex();
    }
    TestIndexDataProvider.activeFieldState
        = new int[TestIndexDataProvider.fields.size()];
    // set all fields active
    Arrays.fill(TestIndexDataProvider.activeFieldState, 1);
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (TestIndexDataProvider.stopWords.contains(term)) {
      return 0;
    }
    int freq = 0;
    for (final Integer docId : getDocumentIds()) {
      if (documentContains(docId, term)) {
        freq++;
      }
    }
    return freq;
  }

  /**
   * Create a simple test index and initialize the {@link TempDiskIndex}.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  private void createIndex()
      throws IOException, ProcessingException {
    TestIndexDataProvider.idx = DBMaker.newTempFileDB()
        .transactionDisable()
        .asyncWriteEnable()
        .asyncWriteFlushDelay(100)
        .mmapFileEnableIfSupported()
        .make().createTreeMap("idx").keySerializer(
            BTreeKeySerializer.TUPLE3).valueSerializer(
            Serializer.LONG).make();

    // generate random document fields
    final int fieldsCount = RandomValue.getInteger(
        TestIndexDataProvider.idxConf.fieldCount[0],
        TestIndexDataProvider.idxConf.fieldCount[1]);
    TestIndexDataProvider.fields = new ArrayList<>(fieldsCount);
    for (int i = 0; i < fieldsCount; i++) {
      TestIndexDataProvider.fields.add(i + "_" + RandomValue.getString(3, 10));
    }

    // create the lucene index
    TestIndexDataProvider.tmpIdx = new TempDiskIndex(
        TestIndexDataProvider.fields.toArray(
            new String[TestIndexDataProvider.fields.size()])
    );

    // set the number of random documents to create
    TestIndexDataProvider.documentsCount = RandomValue.getInteger(
        TestIndexDataProvider.idxConf.docCount[0],
        TestIndexDataProvider.idxConf.docCount[1]);

    LOG.info("Creating a {} sized index with {} documents, {} fields each "
            + "and a maximum of {} terms per field. This may take some time.",
        TestIndexDataProvider.idxConf.toString(),
        TestIndexDataProvider.documentsCount, fieldsCount,
        TestIndexDataProvider.idxConf.docLength[1]
    );

    final int termSeedSize = (int) ((fieldsCount
        * TestIndexDataProvider.documentsCount * TestIndexDataProvider.idxConf
        .docLength[1]) * 0.005);

    // generate a seed of random terms
    LOG.info("Creating term seed with {} terms.", termSeedSize);
    final List<String> seedTermList = new ArrayList<>(termSeedSize);
    while (seedTermList.size() < termSeedSize) {
      seedTermList.add(RandomValue.getString(
          TestIndexDataProvider.idxConf.termLength[0],
          TestIndexDataProvider.idxConf.termLength[1]));
    }

    LOG.info("Creating {} documents.", TestIndexDataProvider.documentsCount);
    final Collection<Integer> latch = new ArrayList<>(
        TestIndexDataProvider.documentsCount);
    for (int doc = 0; doc < TestIndexDataProvider.documentsCount; doc++) {
      latch.add(doc);
    }
    final Map<Integer, String[]> idxMap = DBMaker.newTempTreeMap();
    new Processing(
        new TargetFuncCall<>(
            new CollectionSource<>(latch),
            new DocCreator(idxMap, seedTermList, fieldsCount)
        )
    ).process();
    for (final String[] c : idxMap.values()) {
      TestIndexDataProvider.tmpIdx.addDoc(c);
    }
    TestIndexDataProvider.tmpIdx.flush();
    idxMap.clear();

    // create data path
    final File dataDir = new File(FileUtils.makePath(tmpIdx.getIndexDir()) +
        File.separatorChar + "data");
    if (!dataDir.exists() && !dataDir.mkdirs()) {
      throw new IOException("Failed to create data directory: '" + dataDir
          + "'");
    }

    this.reference = new Reference()
        .setIndexDir(tmpIdx.getIndexDir())
        .setDataDir(dataDir.getCanonicalPath())
        .setDocumentFields(fields);

    TestIndexDataProvider.initialized = true;
  }

  /**
   * Get the Lucene index.
   *
   * @return Lucene index
   */
  public TempDiskIndex getIndex() {
    return TestIndexDataProvider.tmpIdx;
  }

  public static String getIndexConfName() {
    return TestIndexDataProvider.idxConf.name();
  }

  /**
   * Set all fields active.
   */
  private void enableAllFields() {
    Arrays.fill(TestIndexDataProvider.activeFieldState, 1);
    LOG.debug("Enabled all {} fields.",
        TestIndexDataProvider.activeFieldState.length);
  }

  /**
   * Prepare the testing environment.
   *
   * @param newFields Document fields to use
   * @param newStopwords List of stopwords to set
   * @throws IOException Thrown on low-level i/o errors
   */
  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  public void prepareTestEnvironment(
      final Set<String> newFields,
      final Set<String> newStopwords)
      throws IOException {
    final String indexDir = tmpIdx.getIndexDir();

//    final String[] activeFields;
    if (newFields == null) {
      enableAllFields();
//      activeFields = getDocumentFields().toArray(
//          new String[TestIndexDataProvider.activeFieldState.length]);
    } else {
//      activeFields = newFields.toArray(new String[newFields.size()]);
      Arrays.fill(TestIndexDataProvider.activeFieldState, 0);
      // activate single fields
      for (final String field : newFields) {
        setFieldState(field, true);
      }
    }

    if (newStopwords == null) {
      TestIndexDataProvider.stopWords = Collections.<ByteArray>emptySet();
      this.reference.setStopwords(Collections.<String>emptySet());
    } else {
      this.reference.setStopwords(newStopwords);
      TestIndexDataProvider.stopWords = DBMaker.newTempHashSet();
      for (final String stopWord : newStopwords) {
        try {
          TestIndexDataProvider.stopWords.add(new ByteArray(stopWord.getBytes(
              "UTF-8")));
        } catch (UnsupportedEncodingException ex) {
          LOG.error("Error adding stopword '" + stopWord + "'.", ex);
        }
      }
    }

    LOG.info("Preparing Environment. fields={} stopwords={}",
        getDocumentFields(), TestIndexDataProvider.stopWords.size());
  }

  /**
   * Get a list of active fields.
   *
   * @return Collection of active fields
   */
  @Override
  public Set<String> getDocumentFields() {
    final Set<String> fieldNames = new HashSet<>(
        TestIndexDataProvider.fields.size());
    for (int i = 0; i < TestIndexDataProvider.fields.size(); i++) {
      if (TestIndexDataProvider.activeFieldState[i] == 1) {
        fieldNames.add(TestIndexDataProvider.fields.get(i));
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
    for (int i = 0; i < TestIndexDataProvider.fields.size(); i++) {
      if (TestIndexDataProvider.fields.get(i).equals(fieldName)) {
        oldState = TestIndexDataProvider.activeFieldState[i] == 1;
        if (state) {
          TestIndexDataProvider.activeFieldState[i] = 1;
        } else {
          TestIndexDataProvider.activeFieldState[i] = 0;
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
    final Collection<Integer> docIds = new ArrayList<>(
        TestIndexDataProvider.documentsCount);
    for (int i = 0; i < TestIndexDataProvider.documentsCount; i++) {
      docIds.add(i);
    }
    return docIds;
  }

  /**
   * Checks, if a document-id is valid (in index).
   *
   * @param docId Document-id to check
   */
  private void checkDocumentId(final int docId) {
    if (docId < 0 || docId > TestIndexDataProvider.documentsCount - 1) {
      throw new IllegalArgumentException("Illegal document id: " + docId);
    }
  }

  /**
   * Get the overall term-frequency for a specific document.
   *
   * @param docId Document-id to lookup
   * @return overall term-frequency
   */
  private int getDocumentTermFrequency(final int docId) {
    final Map<ByteArray, Long> docTermMap =
        this.reference.getDocumentTermFrequencyMap(docId);
    int docTermCount = 0;
    for (final Number count : docTermMap.values()) {
      docTermCount += count.intValue();
    }
    return docTermCount;
  }

  @Override
  public long getTermFrequency() {
    Long frequency = 0L;

    for (int fieldNum = 0; fieldNum < TestIndexDataProvider.fields.size();
         fieldNum++) {
      if (TestIndexDataProvider.activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < TestIndexDataProvider.documentsCount;
             docId++) {
          final Iterable<ByteArray> docTerms = Fun.filter(TestIndexDataProvider
              .idx.
                  keySet(), fieldNum, docId);
          for (final ByteArray docTerm : docTerms) {
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
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }

    Long frequency = 0L;
    if (TestIndexDataProvider.stopWords.contains(term)) { // skip stopwords
      return frequency;
    }

    for (int fieldNum = 0; fieldNum < TestIndexDataProvider.fields.size();
         fieldNum++) {
      if (TestIndexDataProvider.activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < TestIndexDataProvider.documentsCount;
             docId++) {
          final Long docTermFreq = TestIndexDataProvider.idx.get(Fun.t3(
              fieldNum, docId, term));
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
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }

    if (TestIndexDataProvider.stopWords.contains(term)) { // skip stopwords
      return 0d;
    }

    final Long termFrequency = getTermFrequency(term);
    if (termFrequency == null) {
      return 0;
    }
    return termFrequency.doubleValue() / Long.valueOf(getTermFrequency()).
        doubleValue();
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    return this.reference.getTermSet().iterator();
  }

  @Override
  public Source<ByteArray> getTermsSource() {
    return new CollectionSource<>(this.reference.getTermSet());
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
    return this.reference.getTermSet().size();
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocumentId(docId);
    final int docTermFreq = getDocumentTermFrequency(docId);
    final DocumentModel.DocumentModelBuilder dmBuilder
        = new DocumentModel.DocumentModelBuilder(docId, docTermFreq);
    dmBuilder.setTermFrequency(this.reference.getDocumentTermFrequencyMap
        (docId));
    return dmBuilder.getModel();
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    if (docId == null) {
      throw new IllegalArgumentException("Document id was null.");
    }

    return !(docId < 0 || docId > (TestIndexDataProvider.documentsCount - 1));
  }

  @Override
  public long getDocumentCount() {
    return TestIndexDataProvider.documentsCount;
  }

  @Override
  public boolean documentContains(final int documentId, final ByteArray term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (TestIndexDataProvider.stopWords.contains(term)) { // skip stopwords
      return false;
    }
    checkDocumentId(documentId);
    return reference.getDocumentTermFrequencyMap(documentId).keySet().contains(
        term);
  }

  @Override
  public Long getLastIndexCommitGeneration() {
    return 0L;
  }

  @Override
  public Set<String> getStopwords() {
    return reference.getStopwordsStr();
  }

  public IndexReader getIndexReader()
      throws IOException {
    return tmpIdx.getReader();
  }

  @Override
  public boolean isDisposed() {
    return false; // never
  }

  @Override
  public void dispose() {
    // NOP
  }

  @Override
  public Collection<ByteArray> getDocumentsTermSet(
      final Collection<Integer> docIds) {
    if (docIds == null || docIds.isEmpty()) {
      throw new IllegalArgumentException("Empty document id list.");
    }
    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new HashSet<>();

    for (final Integer docId : uniqueDocIds) {
      terms.addAll(this.reference.getDocumentTermSet(docId, this));
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
    return TestIndexDataProvider.initialized;
  }

  public final class Util {
    /**
     * Picks some (1 to n) terms from the referenceIndex.
     *
     * @return Stop words term collection
     */
    public Set<String> getRandomStopWords() {
      final Iterator<ByteArray> termsIt = TestIndexDataProvider.this
          .getTermsIterator();
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Set<String> stopWords = new HashSet<>();
      while (termsIt.hasNext()) {
        if (RandomValue.getBoolean()) {
          stopWords.add(ByteArrayUtils.utf8ToString(termsIt.next()));
        } else {
          termsIt.next();
        }
      }
      if (stopWords.isEmpty()) {
        stopWords.add(ByteArrayUtils.utf8ToString(new ArrayList<>(
            TestIndexDataProvider.reference.getTermSet()).get(0)));
      }
      return stopWords;
    }

    /**
     * Get a random subset of all available fields.
     *
     * @return Collection of random fields
     */
    public Set<String> getRandomFields() {
      final Set<String> fieldNames;
      if (TestIndexDataProvider.fields.size() == 1) {
        fieldNames = new HashSet<>(1);
        fieldNames.add(TestIndexDataProvider.fields.get(0));
      } else {
        final int[] newFieldState = TestIndexDataProvider.activeFieldState.
            clone();
        // ensure both states are not the same
        while (Arrays.equals(newFieldState,
            TestIndexDataProvider.activeFieldState)) {
          for (int i = 0; i < TestIndexDataProvider.activeFieldState.length;
               i++) {
            newFieldState[i] = RandomValue.getInteger(0, 1);
          }
        }

        fieldNames = new HashSet<>(TestIndexDataProvider.fields.size());
        for (int i = 0; i < TestIndexDataProvider.fields.size(); i++) {
          if (newFieldState[i] == 1) {
            fieldNames.add(TestIndexDataProvider.fields.get(i));
          }
        }

        // lazy backup - add a random field, if none set already
        if (fieldNames.isEmpty()) {
          fieldNames.add(TestIndexDataProvider.fields.
              get(RandomValue.getInteger(0, TestIndexDataProvider.fields.
                  size() - 1)));
        }
      }
      return fieldNames;
    }

    /**
     * Create a query object from the given terms. The query string may contain
     * stopwords.
     *
     * @param queryTerms Terms to use in query
     * @return A query object consisting of the given terms
     * @throws ParseException Thrown on query parsing errors
     */
    public SimpleTermsQuery getQueryObj(final String[] queryTerms)
        throws ParseException, IOException,
               Buildable.BuildableException {
      final TermsQueryBuilder tqb = new TermsQueryBuilder(tmpIdx.getReader(),
          TestIndexDataProvider.this.getDocumentFields());
      tqb.setFields(TestIndexDataProvider.this.getDocumentFields());
      if (queryTerms == null) {
        return tqb.query(TestIndexDataProvider.this.util.getQueryString())
            .build();
      }
      return tqb.query(TestIndexDataProvider.this.util.getQueryString
          (queryTerms)).build();
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
     */
    private String getQueryString(final String[] queryTerms,
        final boolean unique) {
      final String queryStr;

      if (queryTerms != null) {
        if (queryTerms.length == 0) {
          throw new IllegalArgumentException("Query terms where empty.");
        }
        // create query string from passed-in terms
        queryStr = StringUtils.join(queryTerms, " ");
      } else {
        // create a random query string
        if (reference.getTermSet().isEmpty()) {
          throw new IllegalStateException("No terms in index.");
        }

        final List<ByteArray> idxTerms = new ArrayList<>(reference
            .getTermSet());
        final int queryLength = RandomValue.getInteger(
            TestIndexDataProvider.idxConf.queryLength[0],
            TestIndexDataProvider.idxConf.queryLength[1]);

        final Collection<String> terms;

        // check, if terms should be unique
        if (unique) {
          terms = new HashSet<>(queryLength);
        } else {
          terms = new ArrayList<>(queryLength);
        }

        if (TestIndexDataProvider.idxConf.queryLength[0] >= idxTerms.size()) {
          LOG.warn("Minimum query length exceeds unique term count in index. "
              + "Adding all index terms to query.");
          for (final ByteArray term : idxTerms) {
            terms.add(ByteArrayUtils.utf8ToString(term));
          }
        } else if (queryLength >= idxTerms.size()) {
          LOG.warn("Random query length exceeds unique term count in index. "
              + "Adding all index terms to query.");
          for (final ByteArray term : idxTerms) {
            terms.add(ByteArrayUtils.utf8ToString(term));
          }
        } else {
          for (int i = 0; i < queryLength; ) {
            if (terms.add(ByteArrayUtils.utf8ToString(idxTerms.get(RandomValue.
                getInteger(0, idxTerms.size() - 1))))) {
              i++;
            }
          }
        }

        queryStr = StringUtils.join(terms.toArray(new String[terms.
            size()]), " ");
      }

      assert !queryStr.trim().isEmpty() : "Empty query string.";

      LOG.debug("Test query: {}", queryStr);
      return queryStr;
    }

    /**
     * Create a query string from the given terms. The query string may contain
     * stopwords.
     *
     * @param queryTerms Terms to use in query
     * @return A query String consisting of the given terms
     * @throws ParseException Thrown on query parsing errors
     */
    public String getQueryString(final String[] queryTerms)
        throws ParseException {
      return getQueryString(queryTerms, false);
    }

    /**
     * Get a random query matching terms from the documents in index. All terms
     * in the query are unique. The query string may contain stopwords.
     *
     * @return Random query String
     * @throws ParseException Thrown on query parsing errors
     */
    public String getUniqueQueryString()
        throws ParseException {
      return getQueryString(null, true);
    }

    /**
     * Get a random query matching terms from the documents in index. The terms
     * in the query are not unique. The query string may contain stopwords.
     *
     * @return Query generated from random query string
     * @throws ParseException Thrown on query parsing errors
     */
    public SimpleTermsQuery getQueryObj()
        throws ParseException, IOException,
               Buildable.BuildableException {
      return getQueryObj(null);
    }

    /**
     * Get a random query matching terms from the documents in index. The terms
     * in the query are not unique. The query string may contain stopwords.
     *
     * @return Random query string
     * @throws ParseException Thrown on query parsing errors
     */
    public String getQueryString()
        throws ParseException {
      return getQueryString(null, false);
    }
  }

  public static final class Reference {
    private String indexDir;
    private String dataDir;
    private Set<String> fields;
    private Set<ByteArray> stopwordBytes;
    private Set<String> stopwords;

    private Reference setIndexDir(final String dir) {
      if (dir == null || dir.trim().isEmpty()) {
        throw new IllegalArgumentException("Index dir was empty.");
      }
      this.indexDir = dir;
      return this;
    }

    /**
     * Get the file directory where the temporary index resides in.
     *
     * @return Index directory path as string
     */
    public String getIndexDir() {
      return this.indexDir;
    }

    private Reference setDataDir(final String dir) {
      if (dir == null || dir.trim().isEmpty()) {
        throw new IllegalArgumentException("Data dir was empty.");
      }
      this.dataDir = dir;
      return this;
    }

    /**
     * Get the file directory where the working data gets stored.
     *
     * @return Data directory path as string
     */
    public String getDataDir() {
      return this.dataDir;
    }

    private Reference setDocumentFields(final Collection<String> newFields) {
      if (newFields == null || newFields.isEmpty()) {
        throw new IllegalArgumentException("Fields were empty.");
      }
      this.fields = new HashSet<>(newFields.size());
      this.fields.addAll(newFields);
      return this;
    }

    /**
     * Get a list of all document fields available in the index.
     *
     * @return List of all document fields (regardless of their activation
     * state)
     */
    public Set<String> getDocumentFields() {
      return Collections.unmodifiableSet(this.fields);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private Reference setStopwords(final Collection<String> newStopwords)
        throws UnsupportedEncodingException {
      this.stopwordBytes = new HashSet<>(newStopwords.size());
      this.stopwords = new HashSet<>(newStopwords);
      for (final String w : newStopwords) {
        this.stopwordBytes.add(new ByteArray(w.getBytes("UTF-8")));
      }
      return this;
    }

    public Set<ByteArray> getStopwords() {
      return Collections.unmodifiableSet(this.stopwordBytes);
    }

    public Set<String> getStopwordsStr() {
      return Collections.unmodifiableSet(this.stopwords);
    }

    public boolean hasStopwords() {
      return !(this.stopwordBytes == null) || !this.stopwordBytes.isEmpty();
    }

    /**
     * Get a map with <tt>term -> frequency (in document)</tt> values for a
     * specific document.
     *
     * @param docId Document-id to lookup
     * @return Map with <tt>term -> in-document-frequency</tt> values
     */
    public Map<ByteArray, Long> getDocumentTermFrequencyMap(final int docId) {
      if (docId < 0 || docId > TestIndexDataProvider.documentsCount) {
        throw new IllegalArgumentException(
            "Illegal document id " + docId + ".");
      }

      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Map<ByteArray, Long> termFreqMap = new HashMap<>();

      for (int fieldNum = 0; fieldNum < TestIndexDataProvider.fields.size();
           fieldNum++) {
        if (TestIndexDataProvider.activeFieldState[fieldNum] == 1) {
          final Iterable<ByteArray> docTerms = Fun.filter(TestIndexDataProvider
                  .idx.
                      keySet(), fieldNum,
              docId
          );
          for (final ByteArray term : docTerms) {
            if (TestIndexDataProvider.stopWords
                .contains(term)) { // skip stopwords
              continue;
            }
            final Long docTermFreq = TestIndexDataProvider.idx.get(Fun.t3(
                fieldNum, docId, term));
            if (termFreqMap.containsKey(term)) {
              termFreqMap
                  .put(term.clone(), termFreqMap.get(term) + docTermFreq);
            } else {
              termFreqMap.put(term.clone(), docTermFreq);
            }
          }
        }
      }
      return termFreqMap;
    }

    /**
     * Get a unique set of all currently visible index terms.
     *
     * @return Set of all terms in index
     */
    public Collection<ByteArray> getTermSet() {
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Collection<ByteArray> terms = new HashSet<>();

      for (int fieldNum = 0; fieldNum < TestIndexDataProvider.fields.size();
           fieldNum++) {
        if (TestIndexDataProvider.activeFieldState[fieldNum] == 1) {
          for (int docId = 0; docId < TestIndexDataProvider.documentsCount;
               docId++) {
            final Iterable<ByteArray> docTerms = Fun.filter
                (TestIndexDataProvider.idx.
                    keySet(), fieldNum, docId);
            for (final ByteArray docTerm : docTerms) {
              if (TestIndexDataProvider.stopWords
                  .contains(docTerm)) { // skip stopwords
                continue;
              }
              if (docTerm == null) {
                throw new IllegalStateException("Terms was null. doc=" + docId
                    + " field=" + TestIndexDataProvider.fields.get(fieldNum));
              }
              terms.add(docTerm.clone());
            }
          }
        }
      }
      return terms;
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
      for (int fieldNum = 0; fieldNum < TestIndexDataProvider.fields.size();
           fieldNum++) {
        if (TestIndexDataProvider.activeFieldState[fieldNum] == 1) {
          for (int docId = 0; docId < TestIndexDataProvider.documentsCount;
               docId++) {
            final Iterable<ByteArray> docTerms = Fun.filter
                (TestIndexDataProvider.idx.
                    keySet(), fieldNum, docId);
            for (final ByteArray docTerm : docTerms) {
              if (TestIndexDataProvider.stopWords
                  .contains(docTerm)) { // skip stopwords
                continue;
              }
              final long docTermFreqCount = TestIndexDataProvider.idx.get(Fun.
                  t3(fieldNum, docId, docTerm));
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
     * Get a set of unique terms for a document.
     *
     * @param docId Document-id to lookup
     * @param testIndexDataProvider
     * @return List of unique terms in document
     */
    public Collection<ByteArray> getDocumentTermSet(final int docId,
        final TestIndexDataProvider testIndexDataProvider) {
      testIndexDataProvider.checkDocumentId(docId);
      return testIndexDataProvider.reference.getDocumentTermFrequencyMap(docId)
          .keySet();
    }
  }

  /**
   * {@link Processing} {@link Target} creating random documents.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class DocCreator
      extends TargetFuncCall.TargetFunc<Integer> {

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
    private final Map<Integer, String[]> contentCache;

    /**
     * Factory instance creator.
     *
     * @param targetMap Target map to put results in
     * @param newSeedTermList List with terms to add to documents
     * @param newFieldsCount Number of fields to create
     */
    private DocCreator(
        final Map<Integer, String[]> targetMap,
        final List<String> newSeedTermList,
        final int newFieldsCount) {
      super();
      this.seedTermList = newSeedTermList;
      this.fieldsCount = newFieldsCount;
      this.contentCache = targetMap;
    }

    @Override
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public void call(final Integer docId) {
      if (docId == null) {
        return;
      }

      final String[] docContent = new String[this.fieldsCount];

      // create document fields
      for (int field = 0; field < this.fieldsCount; field++) {
        final int fieldTerms = RandomValue.getInteger(idxConf.docLength[0],
            idxConf.docLength[1]);
        final StringBuilder content = new StringBuilder(fieldTerms *
            idxConf.termLength[1]);
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
            fieldTermFreq.put(docTermBytes, new AtomicInteger(1));
          }

          // append term to content
          content.append(docTerm).append(' ');
        }
        // store document field
        docContent[field] = content.toString().trim();

        // store document field term frequency value
        for (final Entry<ByteArray, AtomicInteger> ftfEntry : fieldTermFreq.
            entrySet()) {
          idx.put(Fun.t3(field, docId, ftfEntry.getKey()),
              ftfEntry.getValue().longValue());
        }
      }

      this.contentCache.put(docId, docContent);
    }
  }
}
