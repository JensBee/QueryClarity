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
import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.query.SimpleTermsQuery;
import de.unihildesheim.iw.lucene.util.TempDiskIndex;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.IndexReader;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Temporary (slow) index of random generated documents for testing purposes.
 *
 * @author Jens Bertram
 */
public final class TestIndexDataProvider
    implements IndexDataProvider {

  /**
   * Default index size to use for running tests.
   */
  public static final IndexSize INDEX_SIZE;

  /**
   * Logger instance for this class.
   */
  static final Logger LOG = LoggerFactory.getLogger(
      TestIndexDataProvider.class);

  /**
   * Index field names.
   */
  static final List<String> FIELDS;
  /**
   * Temporary Lucene index held in memory.
   */
  static final TempDiskIndex TEMP_DISK_INDEX;
  /**
   * Directory containing working data.
   */
  static final File DATA_DIR;
  /**
   * Number of documents in index.
   */
  static final int DOCUMENTS_COUNT;

  static {
    try {
      INDEX_SIZE = IndexSize.SMALL;

      IDX = DBMaker.newTempFileDB()
          .deleteFilesAfterClose()
          .closeOnJvmShutdown()
          .transactionDisable()
          .asyncWriteEnable()
          .asyncWriteFlushDelay(100)
          .mmapFileEnableIfSupported()
          .make().createTreeMap("idx")
          .keySerializer(BTreeKeySerializer.TUPLE3)
          .valueSerializer(Serializer.LONG)
          .make();

      // generate random document fields
      final int fieldsCount = RandomValue.getInteger(
          INDEX_SIZE.fieldCount[0], INDEX_SIZE.fieldCount[1]);

      final List<String> newFields = new ArrayList<>(fieldsCount);
      for (int i = 0; i < fieldsCount; i++) {
        newFields.add(i + "_" + RandomValue.getString(3, 10));
      }
      FIELDS = Collections.unmodifiableList(newFields);

      // create the lucene index
      TEMP_DISK_INDEX = new TempDiskIndex(FIELDS.toArray(
          new String[FIELDS.size()])
      );

      // set the number of random documents to create
      DOCUMENTS_COUNT = RandomValue.getInteger(
          INDEX_SIZE.docCount[0], INDEX_SIZE.docCount[1]);

      LOG.info("Creating a {} sized index with {} documents, {} fields each "
              + "and a maximum of {} terms per field. This may take some time.",
          INDEX_SIZE, DOCUMENTS_COUNT, fieldsCount,
          INDEX_SIZE.docLength[1]
      );

      final int termSeedSize = (int) ((double) (fieldsCount
          * DOCUMENTS_COUNT * INDEX_SIZE.docLength[1]) * 0.005);

      // generate a seed of random terms
      LOG.info("Creating term seed with {} terms.", termSeedSize);
      final List<String> seedTermList = new ArrayList<>(termSeedSize);
      while (seedTermList.size() < termSeedSize) {
        seedTermList.add(RandomValue.getString(
            INDEX_SIZE.termLength[0], INDEX_SIZE.termLength[1]));
      }

      LOG.info("Creating {} documents.", DOCUMENTS_COUNT);
      final Collection<Integer> latch = new ArrayList<>(DOCUMENTS_COUNT);
      for (int doc = 0; doc < DOCUMENTS_COUNT; doc++) {
        latch.add(doc);
      }
      final Map<Integer, String[]> idxMap = DBMaker.newTempTreeMap();
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(latch),
              new DocCreator(idxMap, seedTermList, fieldsCount)
          )
      ).process(DOCUMENTS_COUNT);
      for (final String[] c : idxMap.values()) {
        TEMP_DISK_INDEX.addDoc(c);
      }
      TEMP_DISK_INDEX.flush();
      idxMap.clear();

      // create data path
      DATA_DIR = new File(TEMP_DISK_INDEX.getIndexDir(), "data");
      if (!DATA_DIR.exists() && !DATA_DIR.mkdirs()) {
        throw new IOException(
            "Failed to create data directory: '" + DATA_DIR + "'.");
      }
    } catch (final IOException | ProcessingException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Field, Document-id, Term -> Frequency.
   */
  static final ConcurrentNavigableMap<
      Fun.Tuple3<Integer, Integer, ByteArray>, Long> IDX;
  /**
   * Bit-mask storing active fields state. 0 means off 1 on. Index is related to
   * {@link #FIELDS}.
   */
  private final int[] activeFieldState;
  /**
   * List of stop-words to exclude from term frequency calculations. ByteArray.
   */
  private Set<ByteArray> stopwordBytes = Collections.emptySet();
  /**
   * List of stop-words to exclude from term frequency calculations. String.
   */
  private Set<String> stopwordStr = Collections.emptySet();

  /**
   * Create a new test index.
   */
  public TestIndexDataProvider() {
    this.activeFieldState = new int[FIELDS.size()];
    // set all fields active
    Arrays.fill(this.activeFieldState, 1);
  }

  /**
   * Constructor for unit tests.
   *
   * @param fields List of fields to query
   * @param stopwords List of stopwords
   * @throws IOException Thrown on low-level I/O errors
   */
  TestIndexDataProvider(final Iterable<String> fields,
      final Collection<String> stopwords)
      throws IOException {
    this.activeFieldState = new int[FIELDS.size()];
    // set all fields active
    Arrays.fill(this.activeFieldState, 1);
    prepareTestEnvironment(fields, stopwords);
  }

  /**
   * Prepare the testing environment.
   *
   * @param newFields Document fields to use
   * @param newStopwords List of stopwords to set
   * @throws IOException Thrown on low-level i/o errors
   */
  public void prepareTestEnvironment(
      final Iterable<String> newFields,
      final Collection<String> newStopwords)
      throws IOException {

    if (newFields == null) {
      enableAllFields();
    } else {
      Arrays.fill(this.activeFieldState, 0);
      // activate single fields
      for (final String field : newFields) {
        setFieldState(field, true);
      }
    }

    if (newStopwords == null) {
      this.stopwordBytes = Collections.emptySet();
      setStopwords(Collections.<String>emptySet());
    } else {
      setStopwords(newStopwords);
    }

    LOG.info("Preparing Environment. fields={} stopwords({})={}",
        getDocumentFields(), this.stopwordStr.size(), this.stopwordStr);
  }

  /**
   * Set all fields active.
   */
  private void enableAllFields() {
    Arrays.fill(this.activeFieldState, 1);
    LOG.debug("Enabled all {} fields.",
        this.activeFieldState.length);
  }

  /**
   * Set the active state of a field.
   *
   * @param fieldName Name of the field
   * @param state True, to use this field for calculation
   * @return Old state value, or <tt>null</tt> if the field does not exist
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
  private Boolean setFieldState(final String fieldName, final boolean state) {
    Boolean oldState = null;
    for (int i = 0; i < FIELDS.size(); i++) {
      if (FIELDS.get(i).equals(fieldName)) {
        oldState = this.activeFieldState[i] == 1;
        if (state) {
          this.activeFieldState[i] = 1;
        } else {
          this.activeFieldState[i] = 0;
        }
      }
    }
    return oldState;
  }

  /**
   * Get the name of the index configuration in use.
   *
   * @return Configuration name
   */
  public static String getIndexConfName() {
    return INDEX_SIZE.name();
  }

  /**
   * Get the Lucene index.
   *
   * @return Lucene index
   */
  public static TempDiskIndex getIndex() {
    return TEMP_DISK_INDEX;
  }

  /**
   * Get the reader used to access the index.
   *
   * @return Reader
   * @throws IOException Thrown on low-level I/O errors
   */
  public static IndexReader getIndexReader()
      throws IOException {
    return TEMP_DISK_INDEX.getReader();
  }

  /**
   * Get the file directory where the temporary index resides in.
   *
   * @return Index directory path as string
   */
  public static String getIndexDir() {
    return TEMP_DISK_INDEX.getIndexDir();
  }

  /**
   * Get the file directory where the working data gets stored.
   *
   * @return Data directory path as string
   */
  public static String getDataDir() {
    return DATA_DIR.getPath();
  }

  /**
   * Get a list of all document fields available in the index.
   *
   * @return List of all document fields (regardless of their activation state)
   */
  public static Set<String> getAllDocumentFields() {
    return new HashSet<>(FIELDS);
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    Objects.requireNonNull(term);

    if (this.stopwordBytes.contains(term)) {
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
   * Get a {@link StandardAnalyzer} initialized with the current stopwords set.
   *
   * @return Analyzer with current stopwords set
   */
  public Analyzer getAnalyzer() {
    return new StandardAnalyzer(LuceneDefaults.VERSION,
        new CharArraySet(LuceneDefaults.VERSION, this.stopwordStr, true)
    );
  }

  /**
   * Test, if stopwords are set.
   *
   * @return True if stopwords are set
   */
  public boolean hasStopwords() {
    return !(this.stopwordBytes == null || this.stopwordBytes.isEmpty());
  }

  /**
   * Get the flags array representing the active state of all known fields.
   *
   * @return Flags of document fields
   * @see #activeFieldState
   */
  int[] getActiveFieldState() {
    return this.activeFieldState.clone();
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
    for (int fieldNum = 0; fieldNum < FIELDS.size(); fieldNum++) {
      if (getActiveFieldState()[fieldNum] == 1) {
        for (int docId = 0; docId < DOCUMENTS_COUNT; docId++) {
          final Iterable<ByteArray> docTerms = Fun.filter
              (IDX.keySet(), fieldNum, docId);
          for (final ByteArray docTerm : docTerms) {
            if (getStopwordsBytes().contains(docTerm)) { // skip stopwords
              continue;
            }
            final long docTermFreqCount = IDX.get(Fun.t3(fieldNum, docId,
                docTerm));
            for (int docTermFreq = 0; (long) docTermFreq < docTermFreqCount;
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
   * Create a query object from the given terms. The query string may contain
   * stopwords.
   *
   * @param queryTerms Terms to use in query
   * @return A query object consisting of the given terms
   * @throws IOException Thrown on low-level I/O errors
   * @throws Buildable.BuildableException Thrown if building the query fails
   */
  public SimpleTermsQuery getSTQueryObj(final String[] queryTerms)
      throws IOException,
             Buildable.BuildableException {
    final SimpleTermsQuery.Builder tqb =
        new SimpleTermsQuery.Builder(
            TEMP_DISK_INDEX.getReader(),
            this.getDocumentFields());
    tqb.fields(this.getDocumentFields());
    if (queryTerms == null) {
      return tqb.query(this.getQueryString()).analyzer(getAnalyzer()).build();
    }
    return tqb.query(this.getQueryString(queryTerms)).analyzer(getAnalyzer())
        .build();
  }

  /**
   * Get a random query matching terms from the documents in index. The terms in
   * the query are not unique. The query string may contain stopwords.
   *
   * @return Random query string
   */
  public String getQueryString() {
    return getQueryString(null, false);
  }

  /**
   * Picks some (1 to n) terms from the referenceIndex.
   *
   * @return Stop words term collection
   */
  public Set<String> getRandomStopWords() {
    final Iterator<ByteArray> termsIt = this.getTermsIterator();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Set<String> stopwords = new HashSet<>();
    while (termsIt.hasNext()) {
      if (RandomValue.getBoolean()) {
        stopwords.add(ByteArrayUtils.utf8ToString(termsIt.next()));
      } else {
        termsIt.next();
      }
    }
    if (stopwords.isEmpty()) {
      stopwords.add(ByteArrayUtils.utf8ToString(new ArrayList<>(
          this.getTermSet()).get(0)));
    }
    assert !stopwords.isEmpty();
    return stopwords;
  }

  /**
   * Get a random query matching terms from the documents in index. All terms in
   * the query are unique. The query string may contain stopwords.
   *
   * @return Random query String
   */
  public String getUniqueQueryString() {
    return getQueryString(null, true);
  }

  /**
   * Create a query object from the given terms or create a random query, if no
   * terms were given. The query string may contain stopwords.
   *
   * @param queryTerms List of terms to include in the query, or null to create
   * a random query
   * @param unique If true and not terms are given, then query terms are unique
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
      if (this.getTermSet().isEmpty()) {
        throw new IllegalStateException("No terms in index.");
      }

      final List<ByteArray> idxTerms = new ArrayList<>(
          this.getTermSet());
      final int queryLength = RandomValue.getInteger(
          INDEX_SIZE.queryLength[0],
          INDEX_SIZE.queryLength[1]);

      final Collection<String> terms;

      // check, if terms should be unique
      if (unique) {
        terms = new HashSet<>(queryLength);
      } else {
        terms = new ArrayList<>(queryLength);
      }

      if (INDEX_SIZE.queryLength[0] >= idxTerms.size()) {
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

    assert !StringUtils.isStrippedEmpty(queryStr) : "Empty query string.";

    LOG.debug("Test query: {}", queryStr);
    return queryStr;
  }

  /**
   * Get a unique set of all currently visible index terms.
   *
   * @return Set of all terms in index
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public Collection<ByteArray> getTermSet() {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new HashSet<>();

    for (int fieldNum = 0; fieldNum < FIELDS.size(); fieldNum++) {
      if (getActiveFieldState()[fieldNum] == 1) {
        for (int docId = 0; docId < DOCUMENTS_COUNT;
             docId++) {
          final Iterable<ByteArray> docTerms = Fun.filter
              (IDX.keySet(), fieldNum, docId);
          for (final ByteArray docTerm : docTerms) {
            if (getStopwordsBytes().contains(docTerm)) { // skip stopwords
              continue;
            }
            if (docTerm == null) {
              throw new IllegalStateException("Terms was null. doc=" + docId
                  + " field=" + FIELDS.get(fieldNum));
            }
            terms.add(new ByteArray(docTerm));
          }
        }
      }
    }
    return terms;
  }

  /**
   * Get a random subset of all available fields.
   *
   * @return Collection of random fields
   */
  public Set<String> getRandomFields() {
    final Set<String> fieldNames;
    if (FIELDS.size() == 1) {
      fieldNames = Collections.singleton(FIELDS.get(0));
    } else {
      final int[] newFieldState = getActiveFieldState().clone();
      // ensure both states are not the same
      while (Arrays.equals(newFieldState, getActiveFieldState())) {
        for (int i = 0; i < getActiveFieldState().length; i++) {
          newFieldState[i] = RandomValue.getInteger(0, 1);
        }
      }

      fieldNames = new HashSet<>(FIELDS.size());
      for (int i = 0; i < FIELDS.size(); i++) {
        if (newFieldState[i] == 1) {
          fieldNames.add(FIELDS.get(i));
        }
      }

      // lazy backup - add a random field, if none set already
      if (fieldNames.isEmpty()) {
        fieldNames.add(FIELDS.get(RandomValue.getInteger(0, FIELDS.
            size() - 1)));
      }
    }
    return fieldNames;
  }

  /**
   * Create a query string from the given terms. The query string may contain
   * stopwords.
   *
   * @param queryTerms Terms to use in query
   * @return A query String consisting of the given terms
   */
  public String getQueryString(final String[] queryTerms) {
    return getQueryString(queryTerms, false);
  }

  /**
   * Get a random query matching terms from the documents in index. The terms in
   * the query are not unique. The query string may contain stopwords.
   *
   * @return Query generated from random query string
   * @throws IOException Thrown on low-level I/O errors
   * @throws Buildable.BuildableException Thrown if building the query fails
   */
  public SimpleTermsQuery getQueryObj()
      throws IOException, Buildable.BuildableException {
    return getSTQueryObj(null);
  }

  /**
   * Get a set of unique terms for a document.
   *
   * @param docId Document-id to lookup
   * @return List of unique terms in document
   */
  public Collection<ByteArray> getDocumentTermSet(final int docId) {
    checkDocumentId(docId);
    return getDocumentTermFrequencyMap(docId).keySet();
  }

  /**
   * Get the overall term-frequency for a specific document.
   *
   * @param docId Document-id to lookup
   * @return overall term-frequency
   */
  private int getDocumentTermFrequency(final int docId) {
    final Map<ByteArray, Long> docTermMap =
        this.getDocumentTermFrequencyMap(docId);
    int docTermCount = 0;
    for (final Number count : docTermMap.values()) {
      docTermCount += count.intValue();
    }
    return docTermCount;
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
    @SuppressWarnings("PackageVisibleField")
    final int[] fieldCount;
    /**
     * Minimum and maximum number of documents to create.
     */
    @SuppressWarnings("PackageVisibleField")
    final int[] docCount;
    /**
     * Minimum and maximum length of a random generated document (in terms per
     * field).
     */
    @SuppressWarnings("PackageVisibleField")
    final int[] docLength;
    /**
     * Minimum and maximum length of a random term in a document.
     */
    @SuppressWarnings("PackageVisibleField")
    final int[] termLength;
    /**
     * Minimum and maximum length of a random generated query.
     */
    @SuppressWarnings("PackageVisibleField")
    final int[] queryLength;

    /**
     * Initialize the index configuration.
     *
     * @param sizes Size configuration
     */
    IndexSize(final int[] sizes) {
      this.docCount = new int[]{sizes[0], sizes[1]};
      this.fieldCount = new int[]{sizes[2], sizes[3]};
      this.docLength = new int[]{sizes[4], sizes[5]};
      this.termLength = new int[]{sizes[6], sizes[7]};
      this.queryLength = new int[]{sizes[8], sizes[9]};
    }
  }

  /**
   * Get a map with <tt>term -> frequency (in document)</tt> values for a
   * specific document.
   *
   * @param docId Document-id to lookup
   * @return Map with <tt>term -> in-document-frequency</tt> values
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public Map<ByteArray, Long> getDocumentTermFrequencyMap(final int docId) {
    if (docId < 0 || docId > DOCUMENTS_COUNT) {
      throw new IllegalArgumentException(
          "Illegal document id " + docId + ".");
    }

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<ByteArray, Long> termFreqMap = new HashMap<>();

    for (int fieldNum = 0; fieldNum < FIELDS.size();
         fieldNum++) {
      if (getActiveFieldState()[fieldNum] == 1) {
        final Iterable<ByteArray> docTerms =
            Fun.filter(IDX.keySet(), fieldNum, docId
            );
        for (final ByteArray term : docTerms) {
          if (getStopwordsBytes().contains(term)) { // skip stopwords
            continue;
          }
          final Long docTermFreq = IDX.get(Fun.t3(fieldNum, docId, term));
          if (termFreqMap.containsKey(term)) {
            termFreqMap.put(new ByteArray(term),
                termFreqMap.get(term) + docTermFreq);
          } else {
            termFreqMap.put(new ByteArray(term), docTermFreq);
          }
        }
      }
    }
    return termFreqMap;
  }

  /**
   * {@link Processing} {@link Target} creating random documents.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class DocCreator
      extends TargetFuncCall.TargetFunc<Integer> {

    /**
     * Logger instance for this class.
     */
    static final Logger LOCAL_LOG = LoggerFactory.getLogger(
        DocCreator.class);

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
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    DocCreator(
        final Map<Integer, String[]> targetMap,
        final List<String> newSeedTermList,
        final int newFieldsCount) {
      this.seedTermList = newSeedTermList;
      this.fieldsCount = newFieldsCount;
      this.contentCache = targetMap;
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    @Override
    public void call(final Integer docId) {
      if (docId == null) {
        return;
      }

      final String[] docContent = new String[this.fieldsCount];

      // create document fields
      for (int field = 0; field < this.fieldsCount; field++) {
        final int fieldTerms =
            RandomValue.getInteger(INDEX_SIZE_REF.docLength[0],
                INDEX_SIZE_REF.docLength[1]);
        final StringBuilder content = new StringBuilder(fieldTerms *
            INDEX_SIZE_REF.termLength[1]);
        final Map<ByteArray, AtomicInteger> fieldTermFreq
            = new HashMap<>(fieldTerms);

        // create terms for each field
        for (int term = 0; term < fieldTerms; term++) {
          // pick a document term from the seed
          final String docTerm = this.seedTermList.get(RandomValue.getInteger(0,
              this.seedTermList.size() - 1));
          // add it to the list of known terms
          final ByteArray docTermBytes;
          try {
            docTermBytes = new ByteArray(docTerm.getBytes("UTF-8"));
          } catch (final UnsupportedEncodingException ex) {
            LOCAL_LOG.error("Error encoding term. term={}", docTerm);
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
          IDX_REF.put(Fun.t3(field, docId, new ByteArray(ftfEntry.getKey())),
              ftfEntry.getValue().longValue());
        }
      }

      this.contentCache.put(docId, docContent);
    }

    /**
     * Local copy of the index size set in parent class. Need to make this local
     * to allow threads accessing this value. Direct access to parent class
     * field leads to process freezes. Don't know if this is a bug.
     */
    private static final IndexSize INDEX_SIZE_REF = INDEX_SIZE;


    /**
     * Local reference to index map. Same reason as for {@link
     * #INDEX_SIZE_REF}.
     */
    private static final ConcurrentNavigableMap<
        Fun.Tuple3<Integer, Integer, ByteArray>, Long> IDX_REF = IDX;
  }


  /**
   * Set the list of stopwords to use.
   *
   * @param newStopwords New Stopwords
   * @throws UnsupportedEncodingException Thrown, if a stopwords could not be
   * encoded to UTF-8
   */
  private void setStopwords(final Collection<String> newStopwords)
      throws UnsupportedEncodingException {
    this.stopwordBytes = new HashSet<>(newStopwords.size());
    this.stopwordStr = new HashSet<>(newStopwords);
    for (final String w : newStopwords) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final ByteArray wBa = new ByteArray(w.getBytes("UTF-8"));
      this.stopwordBytes.add(wBa);
    }
  }


  /**
   * Get a list of active fields.
   *
   * @return Collection of active fields
   */
  @Override
  public Set<String> getDocumentFields() {
    final Set<String> fieldNames = new HashSet<>(FIELDS.size());
    for (int i = 0; i < FIELDS.size(); i++) {
      if (this.activeFieldState[i] == 1) {
        fieldNames.add(FIELDS.get(i));
      }
    }
    return fieldNames;
  }

  /**
   * Get a collection of all known document-ids.
   *
   * @return All known document-ids
   */
  private static Collection<Integer> getDocumentIds() {
    final Collection<Integer> docIds = new ArrayList<>(DOCUMENTS_COUNT);
    for (int i = 0; i < DOCUMENTS_COUNT; i++) {
      docIds.add(i);
    }
    return docIds;
  }

  /**
   * Checks, if a document-id is valid (in index).
   *
   * @param docId Document-id to check
   */
  static void checkDocumentId(final int docId) {
    if (docId < 0 || docId > DOCUMENTS_COUNT - 1) {
      throw new IllegalArgumentException("Illegal document id: " + docId);
    }
  }


  @Override
  public long getTermFrequency() {
    Long frequency = 0L;

    for (int fieldNum = 0; fieldNum < FIELDS.size();
         fieldNum++) {
      if (this.activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < DOCUMENTS_COUNT;
             docId++) {
          final Iterable<ByteArray> docTerms =
              Fun.filter(IDX.keySet(), fieldNum, docId);
          for (final ByteArray docTerm : docTerms) {
            if (this.stopwordBytes.contains(docTerm)) {
              // skip stopwords
              continue;
            }
            frequency += IDX.get(Fun.t3(fieldNum, docId, docTerm));
          }
        }
      }
    }
    return frequency;
  }

  @SuppressWarnings("ReturnOfNull")
  @Override
  public Long getTermFrequency(final ByteArray term) {
    Objects.requireNonNull(term);

    Long frequency = 0L;
    if (this.stopwordBytes.contains(term)) { // skip stopwords
      return frequency;
    }

    for (int fieldNum = 0; fieldNum < FIELDS.size(); fieldNum++) {
      if (this.activeFieldState[fieldNum] == 1) {
        for (int docId = 0; docId < DOCUMENTS_COUNT; docId++) {
          final Long docTermFreq = IDX.get(Fun.t3(fieldNum, docId, term));
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
    Objects.requireNonNull(term);

    if (this.stopwordBytes.contains(term)) { // skip stopwords
      return 0d;
    }

    final Long termFrequency = getTermFrequency(term);
    if (termFrequency == null) {
      return 0d;
    }
    return termFrequency.doubleValue() / Long.valueOf(getTermFrequency()).
        doubleValue();
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    return this.getTermSet().iterator();
  }

  @Override
  public Source<ByteArray> getTermsSource() {
    return new CollectionSource<>(this.getTermSet());
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
    return (long) this.getTermSet().size();
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocumentId(docId);
    final int docTermFreq = getDocumentTermFrequency(docId);
    final DocumentModel.Builder dmBuilder
        = new DocumentModel.Builder(docId, docTermFreq);
    dmBuilder.setTermFrequency(this.getDocumentTermFrequencyMap(docId));
    return dmBuilder.getModel();
  }

  @Override
  public boolean hasDocument(final int docId) {
    Objects.requireNonNull(docId);

    return !(docId < 0 || docId > (DOCUMENTS_COUNT - 1));
  }

  @Override
  public long getDocumentCount() {
    return (long) DOCUMENTS_COUNT;
  }

  @Override
  public boolean documentContains(final int documentId, final ByteArray term) {
    Objects.requireNonNull(term);

    if (this.stopwordBytes.contains(term)) { // skip stopwords
      return false;
    }
    checkDocumentId(documentId);
    return this.getDocumentTermFrequencyMap(documentId).keySet()
        .contains(
            term);
  }

  @Override
  public Long getLastIndexCommitGeneration() {
    return 0L;
  }

  @Override
  public Set<String> getStopwords() {
    return Collections.unmodifiableSet(this.stopwordStr);
  }

  @Override
  public Set<ByteArray> getStopwordsBytes() {
    return Collections.unmodifiableSet(this.stopwordBytes);
  }

  @Override
  public boolean isClosed() {
    return false; // never
  }

  @Override
  public void close() {
    // activate all fields
    Arrays.fill(this.activeFieldState, 1);
    // unset all stopwords
    this.stopwordBytes = Collections.emptySet();
    try {
      setStopwords(Collections.<String>emptySet());
    } catch (final UnsupportedEncodingException e) {
      // should never happen
      throw new IllegalStateException("Error closing instance.", e);
    }
  }

  @Override
  public Set<ByteArray> getDocumentsTermSet(
      final Collection<Integer> docIds) {
    if (Objects.requireNonNull(docIds).isEmpty()) {
      throw new IllegalArgumentException("Empty document id list.");
    }
    final Collection<Integer> uniqueDocIds = new HashSet<>(docIds);
    // roughly estimate a size
    final Set<ByteArray> terms = new HashSet<>(uniqueDocIds.size() *
        (INDEX_SIZE.docLength[0] / 5));

    for (final Integer docId : uniqueDocIds) {
      checkDocumentId(docId);
      terms.addAll(this.getDocumentTermSet(docId));
    }
    return terms;
  }

  @Override
  public void warmUp() {
    // NOP
  }


}
