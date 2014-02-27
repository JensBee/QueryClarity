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
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
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
  private static final Logger LOG = LoggerFactory.getLogger(
          TestIndex.class);

  /**
   * <tt>Document-id, [each fields content]</tt>.
   */
  private static List<String[]> documents = null;

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

  private static MemoryIndex memIdx;

  private static Query query;

  private QueryParser qParser;

  /**
   * Map: <tt>(prefix -> ([document-id, term, key] -> value))</tt>.
   */
  private Map<String, Map<Tuple.Tuple3<
          Integer, BytesWrap, String>, Object>> prefixMap;

  public TestIndex() throws IOException, ParseException {
    if (!initialized) {
      createIndex();
    }
    this.activeFieldState = new int[fields.size()];
    // set all fields active
    Arrays.fill(this.activeFieldState, 1);

    this.prefixMap = new ConcurrentHashMap<>();
  }

  /**
   * Create a simple test index and initialize the {@link MemoryIndex}.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  private void createIndex() throws IOException {
    LOG.info("Creating index.");
    documents = new ArrayList<>(4);

    documents.add(new String[]{"Lucene in Action"});
    documents.add(new String[]{"Lucene for Dummies"});
    documents.add(new String[]{"Managing Gigabytes"});
    documents.add(new String[]{"The Art of Computer Science"});

    fields = new ArrayList<>(1);
    fields.add("text");

    memIdx = new MemoryIndex(fields.toArray(new String[fields.size()]),
            documents);

    // create a default query
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
            CharArraySet.EMPTY_SET);
    qParser = new QueryParser(LuceneDefaults.VERSION, fields.get(0), analyzer);

    initialized = true;
  }

  /**
   * Get a random query matching terms from the documents in index.
   * @return Random query
   * @throws ParseException Thrown on query parsing errors
   */
  public Query getQuery() throws ParseException {
    final List<BytesWrap> idxTerms = new ArrayList(getTermSet());
    final int queryLength = RandomValue.integer(1, idxTerms.size());
    Collection<String> terms = new HashSet(queryLength);

    for (int i = 0; i < queryLength;) {
      if (terms.add(BytesWrapUtil.bytesWrapToString(idxTerms.get(RandomValue.
              integer(0, idxTerms.size() - 1))))) {
        i++;
      }
    }
    final String queryStr = StringUtils.join(terms.toArray(new String[terms.
            size()]), " ");
    LOG.info("Test query: {}", queryStr);
    return qParser.parse(queryStr);
  }

  /**
   * Get the reader to the {@link MemoryIndex}.
   *
   * @return Index reader
   * @throws IOException Thrown on low-level I/O errors
   */
  public IndexReader getReader() throws IOException {
    return memIdx.getReader();
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
    // a documents-id is simply the index in the documents array
    Collection<Integer> docIds = new ArrayList<>(documents.size());
    for (int i = 0; i < documents.size(); i++) {
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
  private Map<BytesWrap, Number> getDocumentTermFrequencyMap(final int docId) {
    if (docId < 0 || docId > documents.size()) {
      throw new IllegalArgumentException("Illegal document id " + docId + ".");
    }

    final String[] document = documents.get(docId);
    final Map<BytesWrap, Number> termFreqMap = new HashMap<>();

    for (int i = 0; i < document.length; i++) {
      if (activeFieldState[i] == 1) {
        for (String term : document[i].split("\\s+")) {
          final BytesWrap termBw = new BytesWrap(
                  new BytesRef(StringUtils.lowerCase(term)));
          if (termFreqMap.containsKey(termBw)) {
            termFreqMap.put(termBw, termFreqMap.get(termBw).intValue() + 1);
          } else {
            termFreqMap.put(termBw, 1);
          }
        }
      }
    }
    return termFreqMap;
  }

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
    if (docId < 0 || docId > documents.size() - 1) {
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
  Collection<BytesWrap> getTermList() {
    final Collection<BytesWrap> terms = new ArrayList<>();
    for (String[] documentFields : documents) {
      for (int i = 0; i < documentFields.length; i++) {
        if (activeFieldState[i] == 1) {
          for (String term : documentFields[i].split("\\s+")) {
            terms.add(new BytesWrap(
                    new BytesRef(StringUtils.lowerCase(term))));
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
  private Collection<BytesWrap> getTermSet() {
    return new HashSet<>(getTermList());
  }

  /**
   * Remove any custom data stored while using the index.
   */
  public void reset() {
    this.prefixMap = new ConcurrentHashMap<>();
  }

  @Override
  public long getTermFrequency() {
    return getTermList().size();
  }

  @Override
  public Long getTermFrequency(final BytesWrap term) {
    Long frequency = 0L;
    for (BytesWrap idxTerm : getTermList()) {
      if (idxTerm.equals(term)) {
        frequency++;
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
    return (double) termFrequency / (double) getTermFrequency();
  }

  @Override
  public String[] getTargetFields() {
    return fields.toArray(new String[fields.size()]);
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
  public DocumentModel getDocumentModel(int docId) {
    checkDocumentId(docId);
    final int docTermFreq = getDocumentTermFrequency(docId);
    final DocumentModel.DocumentModelBuilder dmBuilder
            = new DocumentModel.DocumentModelBuilder(docId, docTermFreq);
    dmBuilder.setTermFrequency(docTermFreq);
    dmBuilder.setTermFrequency(getDocumentTermFrequencyMap(docId));
    return dmBuilder.getModel();
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    return !(docId < 0 || docId > (documents.size() - 1));
  }

  @Override
  public long getDocumentCount() {
    return documents.size();
  }

  @Override
  public boolean documentContains(final int documentId, final BytesWrap term) {
    checkDocumentId(documentId);
    return getDocumentTermFrequencyMap(documentId).keySet().contains(term);
  }

  @Override
  public void registerPrefix(String prefix) {
    // nothing to do here
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
    try {
      getReader().close();
      memIdx.close();
    } catch (IOException ex) {
      LOG.error("Error while closing memory index.", ex);
    }
  }

  // ---------- TEST ACCESSORS ----------
  /**
   * Get the list of known documents.
   *
   * @return Documents list
   */
  protected static List<String[]> test_getDocuments() {
    return Collections.unmodifiableList(documents);
  }

  /**
   * Get the known fields list
   *
   * @return Fields list
   */
  protected static List<String> test_getFields() {
    return Collections.unmodifiableList(fields);
  }

  /**
   * Get the bit-flags for each field
   *
   * @return Field flags
   */
  protected int[] test_getActiveFieldState() {
    return activeFieldState.clone();
  }

  /**
   * Get the initialized state
   *
   * @return True, if initialized
   */
  public static boolean test_isInitialized() {
    return initialized;
  }

  // ---------- UNSUPPORTED OPERATIONS ----------
  @Override
  public boolean addDocument(final DocumentModel docModel) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateDocument(final DocumentModel docModel) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getProperty(final String prefix, final String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commitHook() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean transactionHookRequest() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void transactionHookRelease() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void transactionHookRoolback() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
