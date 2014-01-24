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

import de.unihildesheim.lucene.LuceneDefaults;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.util.StringUtils;
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
import java.util.Properties;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TestIndex implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(TestIndex.class);
  /**
   * Lucene index.
   */
  private static final Directory INDEX = new RAMDirectory();
  /**
   * Multiplier for relative term frequency inside documents.
   */
  private static final double RTFM_DOCUMENT = 0.6d;
  /**
   * Prevent multiple index initializations.
   */
  private static boolean indexInitialized = false;
  /**
   * Store (field-name -> term -> term-frequency) values.
   */
  private static final Map<String, Map<String, Integer>> fieldTermFrequencies
          = new HashMap();
  /**
   * Store (queryTerm -> (docId, document-probability)) values.
   */
  private static final Map<String, Map<Integer, Double>> docProbabilities
          = new HashMap();
  /**
   * Store (queryTerm -> relative term frequency) values.
   */
  private static final Map<String, Double> relTermFreq = new HashMap();

  /**
   * Fields names of the test Lucene index.
   */
  private static String[] indexFields;

  /**
   * Fields to run queries on.
   */
  private static Set<String> queryFields;

  private static HashMap<Integer, DocumentModel> docModels;

  /**
   * Storage meta data.
   */
  private final Properties storageProp = new Properties();

  /**
   * Store (document-id -> field-name -> lower-cased term list)
   */
  private static final Map<Integer, Map<String, List<String>>> DOCUMENT_INDEX
          = new HashMap();

  public TestIndex() throws IOException {
    // index setup
    final String[] fields = new String[]{"text"};
    final ArrayList<String[]> documents = new ArrayList<String[]>(4);
    documents.add(new String[]{"Lucene in Action"});
    documents.add(new String[]{"Lucene for Dummies"});
    documents.add(new String[]{"Managing Gigabytes"});
    documents.add(new String[]{"The Art of Computer Science"});

    setFields(fields);
    setQueryFields(new HashSet(Arrays.asList(queryFields)));
    initDocModelStore(documents.size());

    LOG.debug("[index] Initializing..");
    this.createIndex(documents);
    this.gatherTestData(documents);
    this.calculateTestData();
    LOG.debug("[index] Initialization finished");
    TestIndex.indexInitialized = true;
  }

  private synchronized void initDocModelStore(int size) {
    TestIndex.docModels = new HashMap(size);
  }

  private synchronized void setQueryFields(final Set<String> fields) {
    TestIndex.queryFields = fields;
  }

  private synchronized void setFields(final String[] fields) {
    TestIndex.indexFields = fields;
  }

  /**
   * Get the probability value for a specific term related to the collection.
   *
   * @param term The term to lookup
   * @return Calculated pct for the given term
   */
  @Override
  public double getRelativeTermFrequency(final String term) {
    return TestIndex.relTermFreq.get(term);
  }

  /**
   * Get probability value for a specific term and document.
   *
   * @param term The term to lookup
   * @param docId The document id to lookup
   * @return Calculated pdt for the given term and document
   */
  public static double getDocumentProbability(final String term,
          final int docId) {
    Map<Integer, Double> probMap = TestIndex.docProbabilities.get(term);
    double probability;

    // may be null, if term is invalid (e.g. not found in index)
    if (probMap == null) {
      probability = 0d;
    } else {
      probability = probMap.get(docId);
    }
    return probability;
  }

  @Override
  public final long getTermFrequency() {
    return getTermFrequency(null);
  }

  @Override
  public long getTermFrequency(final String term) {
    long overallFreq = 0L;

    // iterate over all stored frequency values by field-name
    for (String fieldName : fieldTermFrequencies.keySet()) {
      // check, if current field is included in query
      if (TestIndex.queryFields.contains(fieldName)) {
        // get stored term frequencies for this field
        final Map<String, Integer> termFreqMap = fieldTermFrequencies.get(
                fieldName);

        if (term == null) {
          // no term is given: iterate over all terms
          // and collect the frequency values
          for (Integer termFreq : termFreqMap.values()) {
            overallFreq += termFreq;
          }
        } else {
          // a term is set, try to get only the value for this term
          final Integer termFreq = termFreqMap.get(term);
          if (termFreq != null) {
            overallFreq += termFreqMap.get(term);
          }
        }
      }
    }

    return overallFreq;
  }

  /**
   * Get a unique set of all terms known to this index. This takes into account
   * the currently set query fields.
   *
   * @return Set of all terms
   */
  public Set<String> getTermsSet() {
    Set<String> uniqueTerms = new HashSet();

    // iterate over all stored frequency values by field-name
    for (String fieldName : fieldTermFrequencies.keySet()) {
      // check, if current field is included in query
      if (TestIndex.queryFields.contains(fieldName)) {
        // get all terms from term frequency storage
        uniqueTerms.addAll(fieldTermFrequencies.get(fieldName).keySet());
      }
    }

    return uniqueTerms;
  }

  @Override
  public Iterator<String> getTermsIterator() {
    return getTermsSet().iterator();
  }

  /**
   * Get the number of unique terms in the index. This takes into account the
   * currently set query fields.
   *
   * @return The number of unique terms in the index
   */
  public int getTermCount() {
    return getTermsSet().size();
  }

  /**
   * Get all terms for a document by id. This takes into account the currently
   * set query fields.
   *
   * @param docId
   * @return
   */
  private static List<String> getDocumentTerms(final int docId) {
    final List<String> docTerms = new ArrayList();

    final Map<String, List<String>> docFields = DOCUMENT_INDEX.get(
            docId);
    for (String field : queryFields) {
      docTerms.addAll(docFields.get(field));
    }

    return docTerms;
  }

  /**
   * Get all document-ids for documents matching with one of the given terms.
   * This takes into account the currently set query fields.
   *
   * @param terms Terms to match against
   * @return List of document ids matching at least one of the given terms
   */
  public static Collection<DocumentModel> getDocumentModelsMatching(
          final Collection<String> terms) {
    Collection<DocumentModel> docIds = new HashSet(DOCUMENT_INDEX.size());

    // iterate over all documents
    for (Integer docId : DOCUMENT_INDEX.keySet()) {

      final List<String> docTerms = getDocumentTerms(docId);
      for (String term : terms) {
        if (docTerms.contains(term)) {
          // match - add doc and proceed with next document
          docIds.add(docModels.get(docId));
          break;
        }
      }
    }

    return docIds;
  }

  /**
   * Get the models for all documents in the index.
   *
   * @return Models of all documents in the index
   */
  public static Collection<DocumentModel> getDocumentModels() {
    return docModels.values();
  }

  /**
   * Create the simple in-memory test index.
   *
   * @param documents Documents to add to the index
   * @throws IOException Thrown on low-level I/O errors
   */
  private void createIndex(final ArrayList<String[]> documents) throws
          IOException {
    final StandardAnalyzer analyzer = new StandardAnalyzer(
            LuceneDefaults.VERSION, CharArraySet.EMPTY_SET);
    final IndexWriterConfig config
            = new IndexWriterConfig(LuceneDefaults.VERSION, analyzer);

    // index documents
    int newIdx = 0;
    try (IndexWriter writer = new IndexWriter(INDEX, config)) {
      for (String[] doc : documents) {
        LOG.info("[index] Adding document"
                + " docId={} content='{}'", newIdx++, doc);
        addDoc(writer, doc);
      }
      writer.close();
      LOG.info("[index] Added {} documents to index", documents.size());
    }
  }

  /**
   * Gather index statistics needed for evaluating test results. Test data will
   * be calculated for all available fields regardless if they're queried. The
   * decision, which data to use is up to the higher level functions.
   *
   * @param documents Documents that were added to the index
   */
  private void gatherTestData(final ArrayList<String[]> documents) {
    String[] fieldTokens; // raw tokens of a field
    String fieldName; // name of the current field
    String[] doc; // fields of the current document
    // term character enumeration
    char[] docTermChars;
    // stores field->lowercased-terms for each individual document
    Map<String, List<String>> docFieldTermMap;
    // stores lowercased-terms for each individual document and field
    List<String> docFieldLcTermList;
    // stores fields term->termfrequency for all documents
    Map<String, Integer> idxFieldTermFreq;

    // iterate over all documents in index
    for (int docId = 0; docId < documents.size(); docId++) {
      doc = documents.get(docId);
      docFieldTermMap = new HashMap(indexFields.length);
      DOCUMENT_INDEX.put(docId, docFieldTermMap);
      docModels.put(docId, new TestDocumentModel(docId));

      // iterate over all fields in document
      for (int docFieldNum = 0; docFieldNum < doc.length; docFieldNum++) {
        fieldName = indexFields[docFieldNum];
        fieldTokens = StringUtils.lowerCase(doc[docFieldNum].trim()).split(
                "\\s+");

        docFieldLcTermList = new ArrayList(fieldTokens.length);

        // get/init term->frequency storage
        idxFieldTermFreq = fieldTermFrequencies.get(fieldName);
        if (idxFieldTermFreq == null) {
          // initial size is just a guess
          idxFieldTermFreq = new HashMap((int) (fieldTokens.length * 0.3));
          fieldTermFrequencies.put(fieldName, idxFieldTermFreq);
        }

        // iterate over all tokens in field
        for (String token : fieldTokens) {
          // manual transform to lowercase to avoid locale problems
          docTermChars = token.toCharArray();
          for (int j = 0; j < docTermChars.length; j++) {
            docTermChars[j] = Character.toLowerCase(docTermChars[j]);
          }
          // string is now all lower case
          token = new String(docTermChars);

          // store lower-cased token
          docFieldLcTermList.add(token);

          // update count for current token
          if (idxFieldTermFreq.containsKey(token)) {
            final int tokenCount = idxFieldTermFreq.get(token) + 1;
            idxFieldTermFreq.put(token, tokenCount);
          } else {
            idxFieldTermFreq.put(token, 1);
          }
        }

        // store lower-cases tokens for this document field
        docFieldTermMap.put(fieldName, docFieldLcTermList);
      }
    }

    if (LOG.isTraceEnabled()) {
      for (String term : getTermsSet()) {
        LOG.trace("[index] Frequency term={} freq={}", term,
                getTermFrequency(term));
      }
      LOG.trace("[index] Frequency of all terms in index freq={}", this.
              getTermFrequency());
    }
  }

  /**
   * Calculate document probability for each term in the index. This takes into
   * account the currently set query fields.
   */
  private void calcPDT() {
    LOG.debug("Calculating test data (pdt)");
    List docTerms; // list of terms in document
    double pdt; // document probability for a term and document
    int ftd; // frequency of current term in document
    int fd; // number of terms in document
    long ft; // frequency of term in index
    final Long f = this.getTermFrequency(); // frequency of all terms in index
    double fc;

    // store term-related probability values for each document
    HashMap<Integer, Double> probMap;
    // iterate over all terms in index
    for (String term : getTermsSet()) {
      ft = getTermFrequency(term);
      fc = ((double) ft / (double) f) * (1 - RTFM_DOCUMENT);
      probMap = new HashMap(DOCUMENT_INDEX.size()); // init storage
      // iterate over all documents in index
      // and calculate pdt for the current term and document

      for (Integer docId : DOCUMENT_INDEX.keySet()) {
        docTerms = getDocumentTerms(docId);
        ftd = Collections.frequency(docTerms, term);
        fd = docTerms.size();
        pdt = ((double) ftd / (double) fd) * RTFM_DOCUMENT;
        pdt += fc;
        probMap.put(docId, pdt); // store value for current document
        LOG.info("[pdt] docId={} term={} ftd={} fd={} ft={} f={} pdt={}",
                docId, term, ftd, fd, ft, f, pdt);
      }
      // save calculated values with associated term
      docProbabilities.put(term, probMap);
    }
  }

  /**
   * Calculate collection probability for each term in the index. This takes
   * into account the currently set query fields.
   */
  private void calcPCT() {
    LOG.debug("Calculating test data (pct)");
    long ft; // frequency of term in index
    final Long f = getTermFrequency(); // frequency of all terms in index
    double pct; // collection probability for a term

    for (String term : getTermsSet()) {
      ft = getTermFrequency(term);
      pct = (double) ft / (double) f;
      relTermFreq.put(term, pct);
      LOG.info("[pct] term={} ft={} f={} pct={}", term, ft, f, pct);
    }
  }

  /**
   * Calculate the query probability for the given query, the current term and
   * the list of feedback document. This takes into account the currently set
   * query fields.
   *
   * @param query The query that was run
   * @param term The current term of the query to calculate for
   * @param usedDocModels The feedback document models to use for calculation
   * @return Calculated pqt value
   */
  public static double calcPQT(final Set<String> query, final String term,
          final Collection<DocumentModel> usedDocModels) {
    int docId;
    double pqt = 0d;

    // used for trace output
    final StringBuilder sb = new StringBuilder(200);

    for (DocumentModel doc : usedDocModels) {
      docId = doc.getDocId();
      if (LOG.isTraceEnabled()) {
        sb.append("[pqt] term=").append(term).append(" doc=").append(docId).
                append(" calc=");
      }

      Double pdt = 0d;
      // get pdt for current query term
      try {
        pdt = docProbabilities.get(term).get(docId);
      } catch (NullPointerException e) {
        // this should not happen, because we only should use terms
        // from the collection
        throw new IllegalArgumentException("[pdt] for term=" + term
                + " was 0. This should only happen, if you passed in a term "
                + "not found in the collection.");
      }
      if (LOG.isTraceEnabled()) {
        sb.append("(").append(term).append(")").append(pdt);
      }

      // get pdt for all terms in the query
      for (String qTerm : query) {
        try {
          pdt *= docProbabilities.get(qTerm).get(docId);
          if (LOG.isTraceEnabled()) {
            sb.append(" * (").append(qTerm).append(")").append(
                    docProbabilities.get(qTerm).get(docId));
          }
        } catch (NullPointerException e) {
          // this should not happen, because we only should use terms
          // from the collection
          throw new IllegalArgumentException("[pdt] for term=" + qTerm
                  + " was 0. This should only happen, if you passed in a term "
                  + "not found in the collection.");
        }
      }

      if (LOG.isTraceEnabled()) {
        LOG.debug(sb.toString());
        sb.delete(0, sb.length()); // clear
      }
      pqt += pdt;
    }
    LOG.trace("[pqt] q={} t={} p={} (using {} feedback documents)", query, term,
            pqt, usedDocModels.size());
    return pqt;
  }

  /**
   * This takes into account the currently set query fields.
   *
   * @param terms Terms to use for calculation. This may be either all terms in
   * the collection or only the terms used in the (rewritten) query.
   * @param query Terms used in the original/rewritten query
   * @param usedDocModels Document models to use for calculation
   * @return The query clarity score
   */
  public double calcClarity(final Set<String> terms,
          final Set<String> query,
          final Collection<DocumentModel> usedDocModels) {
    double pqt;
    double clarity = 0d;
    double log; // result of the logarithmic part of the calculation formular

    Double colProb;
    for (String term : terms) {
      pqt = TestIndex.calcPQT(query, term, usedDocModels);

      // try get collection Stringprobability for a term
      colProb = relTermFreq.get(term);
      if (colProb == null) {
        colProb = 0d;
      }

      log = (Math.log(pqt) / Math.log(2)) / (Math.log(colProb) / Math.log(2));
      clarity += (pqt * log);
    }
    return clarity;
  }

  /**
   * Calculate expected results. This takes into account the currently set query
   * fields.
   */
  private void calculateTestData() {
    this.calcPDT();
    this.calcPCT();
  }

  /**
   * Add a document to the index.
   *
   * @param writer Index writer instance
   * @param content Content of the document
   * @throws IOException Thrown on low-level I/O errors
   */
  private void addDoc(final IndexWriter writer, final String[] content)
          throws IOException {
    Document doc = new Document();
    for (int i = 0; i < content.length; i++) {
      doc.add(new VecTextField(TestIndex.indexFields[i], content[i],
              Field.Store.YES));
    }
    writer.addDocument(doc);
  }

  /**
   * Get the index reader used by this instance.
   *
   * @return Index reader used by this instance
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  public IndexReader getReader() throws IOException {
    return DirectoryReader.open(INDEX);
  }

  @Override
  public String[] getTargetFields() {
    return TestIndex.queryFields.toArray(
            new String[TestIndex.queryFields.size()]);
  }

  private long getTermFrequency(final int documentId) {
    Map<String, List<String>> docFieldTerms = DOCUMENT_INDEX.get(documentId);
    long freq = 0L;
    for (String fieldName : getTargetFields()) {
      freq += docFieldTerms.get(fieldName).size();
    }
    return freq;
  }

  private long getTermFrequency(final int documentId, final String term) {
    Map<String, List<String>> docFieldTerms = DOCUMENT_INDEX.get(documentId);
    long freq = 0l;
    for (String fieldName : getTargetFields()) {
      for (String currentTerm : docFieldTerms.get(fieldName)) {
        if (term.equals(currentTerm)) {
          freq++;
        }
      }
    }
    return freq;
  }

  @Override
  public DocumentModel getDocumentModel(final int documentId) {
    return docModels.get(documentId);
  }

  @Override
  public void dispose() {
    // empty
  }

  @Override
  public Iterator<DocumentModel> getDocModelIterator() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DocumentModel removeDocumentModel(final int docId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void addDocumentModel(final DocumentModel documentModel) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getDocModelCount() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setProperty(final String prefix, final String key,
          final String value) {
    storageProp.setProperty(prefix + "_" + key, value);
  }

  @Override
  public String getProperty(final String prefix, final String key) {
    return storageProp.getProperty(prefix + "_" + key);
  }

  @Override
  public String getProperty(String prefix, String key, String defaultValue) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getTermsCount() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private class TestDocumentModel implements DocumentModel {

    private final int docId;

    public TestDocumentModel(final int documentId) {
      this.docId = documentId;
    }

    @Override
    public long getTermFrequency() {
      return TestIndex.this.getTermFrequency(this.docId);
    }

    @Override
    public long getTermFrequency(final String term) {
      return TestIndex.this.getTermFrequency(this.docId, term);
    }

    @Override
    public int getDocId() {
      return this.docId;
    }

    @Override
    public boolean containsTerm(final String term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Number getTermData(final String term, final String key) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DocumentModel setDocId(final int documentId) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DocumentModel create(final int documentId, final int termsCount) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DocumentModel addTermFrequency(final String term,
            final long frequency) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DocumentModel addTermData(final String term, final String key,
            final Number value) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setTermFrequency(String term, long frequency) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void lock() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void unlock() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
