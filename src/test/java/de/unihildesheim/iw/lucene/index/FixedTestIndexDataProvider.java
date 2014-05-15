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
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.TempDiskIndex;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import org.mapdb.Fun;

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
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A fixed temporary Lucene index. It's purpose is to provide a static, well
 * known index to verify calculation results.
 * <p/>
 * This {@link IndexDataProvider} implementation does not support any switching
 * of document fields nor does it respect any stopwords (they will be ignored).
 *
 * @author Jens Bertram
 */
public final class FixedTestIndexDataProvider
    implements IndexDataProvider {

  /**
   * Number of fields per document.
   */
  static final int FIELD_COUNT = 3;

  /**
   * Document fields.
   */
  private static final String[] DOC_FIELDS;

  static {
    DOC_FIELDS = new String[FIELD_COUNT];
    // create field names
    for (int i = 0; i < FIELD_COUNT; i++) {
      DOC_FIELDS[i] = "fld_" + i;
    }
  }

  /**
   * Temporary Lucene index held in memory.
   */
  private static final TempDiskIndex TMP_IDX;

  static {
    try {
      TMP_IDX = new TempDiskIndex(DOC_FIELDS);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Number of documents in the index.
   */
  public static final int DOC_COUNT = 10;

  /**
   * Document contents.
   */
  public static final List<String[]> DOCUMENTS;

  static {
    // create 10 "lorem ipsum" documents with 3 fields
    DOCUMENTS = new ArrayList<>();
    // doc 0
    DOCUMENTS.add(new String[]{
        "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean " +
            "commodo ligula eget dolor. Aenean massa. Cum sociis natoque " +
            "penatibus et magnis dis parturient montes, " +
            "nascetur ridiculus mus.",
        "Donec quam felis, ultricies nec, pellentesque eu, pretium quis, " +
            "sem. Nulla consequat massa quis enim. Donec pede justo, " +
            "fringilla vel, aliquet nec, vulputate eget, arcu.",
        "In enim justo, rhoncus ut, imperdiet a, venenatis vitae, " +
            "justo. Nullam dictum felis eu pede mollis pretium. Integer " +
            "tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean " +
            "vulputate eleifend tellus."
    });
    // doc 1
    DOCUMENTS.add(new String[]{
        "Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, " +
            "enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, " +
            "tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque" +
            " rutrum. Aenean imperdiet.",
        "Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi" +
            ". Nam eget dui. Etiam rhoncus. Maecenas tempus, " +
            "tellus eget condimentum rhoncus, sem quam semper libero, " +
            "sit amet adipiscing sem neque sed ipsum.",
        "Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, " +
            "lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae " +
            "sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit" +
            " amet orci eget eros faucibus tincidunt."
    });
    // doc 2
    DOCUMENTS.add(new String[]{
        "Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis" +
            " magna. Sed consequat, leo eget bibendum sodales, " +
            "augue velit cursus nunc, quis gravida magna mi a libero. Fusce " +
            "vulputate eleifend sapien.",
        "Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, " +
            "metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis" +
            " hendrerit fringilla.",
        "Vestibulum ante ipsum primis in faucibus orci luctus et ultrices " +
            "posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. " +
            "Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, " +
            "imperdiet nec, imperdiet iaculis, ipsum."
    });
    // doc 3
    DOCUMENTS.add(new String[]{
        "Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, " +
            "consectetuer eget, posuere ut, mauris. Praesent adipiscing. " +
            "Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. " +
            "Vestibulum volutpat pretium libero. Cras id dui.",
        "Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, " +
            "ultricies sit amet, nonummy id, imperdiet feugiat, " +
            "pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec " +
            "sem in justo pellentesque facilisis.",
        "Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor," +
            " tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula " +
            "sapien, tincidunt non, euismod vitae, posuere imperdiet, " +
            "leo. Maecenas malesuada."
    });
    // doc 4
    DOCUMENTS.add(new String[]{
        "Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec" +
            " posuere vulputate arcu. Phasellus accumsan cursus velit.",
        "Vestibulum ante ipsum primis in faucibus orci luctus et ultrices " +
            "posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, " +
            "elit erat euismod orci, ac placerat dolor lectus quis orci. " +
            "Phasellus consectetuer vestibulum elit.",
        "Aenean tellus metus, bibendum sed, posuere ac, mattis non, " +
            "nunc. Vestibulum fringilla pede sit amet augue. In turpis. " +
            "Pellentesque posuere. Praesent turpis."
    });
    // doc 5
    DOCUMENTS.add(new String[]{
        "Aenean posuere, tortor sed cursus feugiat, nunc augue blandit nunc, " +
            "eu sollicitudin urna dolor sagittis lacus. Donec elit libero, " +
            "sodales nec, volutpat a, suscipit non, turpis. Nullam sagittis.",
        "Suspendisse pulvinar, augue ac venenatis condimentum, " +
            "sem libero volutpat nibh, nec pellentesque velit pede quis nunc." +
            " Vestibulum ante ipsum primis in faucibus orci luctus et " +
            "ultrices posuere cubilia Curae; Fusce id purus. Ut varius " +
            "tincidunt libero.",
        "Phasellus dolor. Maecenas vestibulum mollis diam. Pellentesque ut " +
            "neque. Pellentesque habitant morbi tristique senectus et netus " +
            "et malesuada fames ac turpis egestas. In dui magna, " +
            "posuere eget, vestibulum et, tempor auctor, justo."
    });
    // doc 6
    DOCUMENTS.add(new String[]{
        "In ac felis quis tortor malesuada pretium. Pellentesque auctor neque" +
            " nec urna. Proin sapien ipsum, porta a, auctor quis, euismod ut," +
            " mi. Aenean viverra rhoncus pede. Pellentesque habitant morbi " +
            "tristique senectus et netus et malesuada fames ac turpis egestas.",
        "Ut non enim eleifend felis pretium feugiat. Vivamus quis mi. " +
            "Phasellus a est. Phasellus magna. In hac habitasse platea " +
            "dictumst. Curabitur at lacus ac velit ornare lobortis. Curabitur" +
            " a felis in nunc fringilla tristique. Morbi mattis ullamcorper " +
            "velit.",
        "Phasellus gravida semper nisi. Nullam vel sem. Pellentesque libero " +
            "tortor, tincidunt et, tincidunt eget, semper nec, " +
            "quam. Sed hendrerit. Morbi ac felis. Nunc egestas, " +
            "augue at pellentesque laoreet, felis eros vehicula leo, " +
            "at malesuada velit leo quis pede."
    });
    // doc 7
    DOCUMENTS.add(new String[]{
        "Donec interdum, metus et hendrerit aliquet, " +
            "dolor diam sagittis ligula, eget egestas libero turpis vel mi. " +
            "Nunc nulla. Fusce risus nisl, viverra et, tempor et, pretium in," +
            " sapien. Donec venenatis vulputate lorem. Morbi nec metus.",
        "Phasellus blandit leo ut odio. Maecenas ullamcorper, " +
            "dui et placerat feugiat, eros pede varius nisi, " +
            "condimentum viverra felis nunc et lorem. Sed magna purus, " +
            "fermentum eu, tincidunt eu, varius ut, felis. In auctor lobortis" +
            " lacus.",
        "Quisque libero metus, condimentum nec, tempor a, commodo mollis, " +
            "magna. Vestibulum ullamcorper mauris at ligula. Fusce fermentum." +
            " Nullam cursus lacinia erat. Praesent blandit laoreet nibh. " +
            "Fusce convallis metus id felis luctus adipiscing."
    });
    // doc 8
    DOCUMENTS.add(new String[]{
        "Pellentesque egestas, neque sit amet convallis pulvinar, " +
            "justo nulla eleifend augue, ac auctor orci leo non est. Quisque " +
            "id mi. Ut tincidunt tincidunt erat. Etiam feugiat lorem non " +
            "metus. Vestibulum dapibus nunc ac augue. Curabitur vestibulum " +
            "aliquam leo.",
        "Praesent egestas neque eu enim. In hac habitasse platea dictumst. " +
            "Fusce a quam. Etiam ut purus mattis mauris sodales aliquam. " +
            "Curabitur nisi. Quisque malesuada placerat nisl. Nam ipsum " +
            "risus, rutrum vitae, vestibulum eu, molestie vel, lacus.",
        "Sed augue ipsum, egestas nec, vestibulum et, malesuada adipiscing, " +
            "dui. Vestibulum facilisis, purus nec pulvinar iaculis, " +
            "ligula mi congue nunc, vitae euismod ligula urna in dolor. " +
            "Mauris sollicitudin fermentum libero. Praesent nonummy mi in " +
            "odio. Nunc interdum lacus sit amet orci."
    });
    // doc 9
    DOCUMENTS.add(new String[]{
        "Vestibulum rutrum, mi nec elementum vehicula, " +
            "eros quam gravida nisl, id fringilla neque ante vel mi. Morbi " +
            "mollis tellus ac sapien. Phasellus volutpat, " +
            "metus eget egestas mollis, lacus lacus blandit dui, " +
            "id egestas quam mauris ut lacus. Fusce vel dui.",
        "Sed in libero ut nibh placerat accumsan. Proin faucibus arcu quis " +
            "ante. In consectetuer turpis ut velit. Nulla sit amet est. " +
            "Praesent metus tellus, elementum eu, semper a, adipiscing nec, " +
            "purus. Cras risus ipsum, faucibus ut, ullamcorper id, varius ac," +
            " leo. Suspendisse feugiat.",
        "Suspendisse enim turpis, dictum sed, iaculis a, condimentum nec, " +
            "nisi. Praesent nec nisl a purus blandit viverra. Praesent ac " +
            "massa at ligula laoreet iaculis. Nulla neque dolor, " +
            "sagittis eget, iaculis quis, molestie non, " +
            "velit. Mauris turpis nunc, blandit et, volutpat molestie, " +
            "porta ut, ligula. Fusce pharetra convallis urna. Quisque ut nisi" +
            ". Donec mi odio, faucibus at, scelerisque quis,"
    });

    // add documents to index
    for (String[] doc : DOCUMENTS) {
      try {
        TMP_IDX.addDoc(doc);
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }

  /**
   * Field number, Document number, Term -> Frequency.
   */
  private static final NavigableMap<Fun.Tuple3<Integer, Integer,
      ByteArray>, Long> IDX;

  static {
    IDX = new TreeMap<>();
    final Locale locale = Locale.ENGLISH; // not english text, but works

    // create the index: go through all documents..
    for (int docId = 0; docId < DOCUMENTS.size(); docId++) {
      final String[] doc = DOCUMENTS.get(docId);
      // ..and all fields
      for (int fieldNum = 0; fieldNum < doc.length; fieldNum++) {
        final String content = doc[fieldNum];
        // count words per field content
        Map<String, Integer> wc = StringUtils.countWords(content, locale);
        for (Map.Entry<String, Integer> entry : wc.entrySet()) {
          try {
            // store result
            IDX.put(
                Fun.t3(fieldNum, docId, new ByteArray(entry.getKey().getBytes
                    ("UTF-8"))), entry.getValue().longValue()
            );
          } catch (UnsupportedEncodingException e) {
            throw new ExceptionInInitializerError(e);
          }
        }
      }
    }
  }

  /**
   * Store the singleton instance.
   */
  private static final FixedTestIndexDataProvider INSTANCE = new
      FixedTestIndexDataProvider();

  /**
   * Empty constructor.
   */
  private FixedTestIndexDataProvider() {
  }

  /**
   * Get the singleton instance.
   *
   * @return Instance
   */
  public static FixedTestIndexDataProvider getInstance() {
    return INSTANCE;
  }

  /**
   * Checks, if a document-id is valid (in index).
   *
   * @param docId Document-id to check
   */
  private void checkDocumentId(final int docId) {
    if (!hasDocument(docId)) {
      throw new IllegalArgumentException("Illegal document id: " + docId);
    }
  }

  @Override
  public long getTermFrequency() {
    long frequency = 0;
    for (int fieldNum = 0; fieldNum < FIELD_COUNT; fieldNum++) {
      for (int docId = 0; docId < DOC_COUNT; docId++) {
        final Iterable<ByteArray> docTerms = Fun.filter(
            (NavigableSet<Fun.Tuple3<Integer, Integer, ByteArray>>) IDX
                .keySet(), fieldNum, docId
        );
        for (final ByteArray docTerm : docTerms) {
          frequency += IDX.get(Fun.t3(fieldNum, docId, docTerm));
        }
      }
    }
    return frequency;
  }

  @Override
  public void warmUp()
      throws DataProviderException {
    // NOP
  }

  @Override
  public Long getTermFrequency(final ByteArray term) {
    Long frequency = 0L;
    for (int fieldNum = 0; fieldNum < FIELD_COUNT; fieldNum++) {
      for (int docId = 0; docId < DOC_COUNT; docId++) {
        final Long docTermFreq = IDX.get(Fun.t3(fieldNum, docId, term));
        if (docTermFreq != null) {
          frequency += docTermFreq;
        }
      }
    }
    return frequency;
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    int freq = 0;
    for (int docId = 0; docId < DOC_COUNT; docId++) {
      if (documentContains(docId, term)) {
        freq++;
      }
    }
    return freq;
  }

  @Override
  public double getRelativeTermFrequency(final ByteArray term) {
    final Long termFrequency = getTermFrequency(term);
    if (termFrequency == null) {
      return 0;
    }
    return termFrequency.doubleValue() / Long.valueOf(getTermFrequency()).
        doubleValue();
  }

  @Override
  public void dispose() {
    // NOP
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    final List<Integer> docIds = new ArrayList<>(DOC_COUNT);
    for (int i = 0; i < DOC_COUNT; i++) {
      docIds.add(i);
    }
    return getDocumentsTermSet(docIds).iterator();
  }

  @Override
  public Source<ByteArray> getTermsSource() {
    final List<Integer> docIds = new ArrayList<>(DOC_COUNT);
    for (int i = 0; i < DOC_COUNT; i++) {
      docIds.add(i);
    }
    return new CollectionSource<>(getDocumentsTermSet(docIds));
  }

  @Override
  public Iterator<Integer> getDocumentIdIterator() {
    final List<Integer> docIds = new ArrayList<>(DOC_COUNT);
    for (int i = 0; i < DOC_COUNT; i++) {
      docIds.add(i);
    }
    return docIds.iterator();
  }

  @Override
  public Source<Integer> getDocumentIdSource() {
    final List<Integer> docIds = new ArrayList<>(DOC_COUNT);
    for (int i = 0; i < DOC_COUNT; i++) {
      docIds.add(i);
    }
    return new CollectionSource<>(docIds);
  }

  @Override
  public long getUniqueTermsCount()
      throws DataProviderException {
    final List<Integer> docIds = new ArrayList<>(DOC_COUNT);
    for (int i = 0; i < DOC_COUNT; i++) {
      docIds.add(i);
    }
    return getDocumentsTermSet(docIds).size();
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocumentId(docId);
    final DocumentModel.Builder dmBuilder
        = new DocumentModel.Builder(docId);

    final Map<ByteArray, Long> tfMap = new HashMap<>();

    for (int fieldNum = 0; fieldNum < FIELD_COUNT; fieldNum++) {
      final Iterable<ByteArray> docTerms = Fun.filter(
          (NavigableSet<Fun.Tuple3<Integer, Integer, ByteArray>>)
              IDX.keySet(), fieldNum, docId
      );
      for (final ByteArray docTerm : docTerms) {
        if (tfMap.containsKey(docTerm)) {
          tfMap.put(docTerm, tfMap.get(docTerm) + 1);
        } else {
          tfMap.put(docTerm, 1L);
        }
      }
    }
    dmBuilder.setTermFrequency(tfMap);
    return dmBuilder.getModel();
  }

  @Override
  public boolean hasDocument(final int docId) {
    return !(docId < 0 || docId > (DOC_COUNT - 1));
  }

  @Override
  public Set<ByteArray> getDocumentsTermSet(
      final Collection<Integer> docIds) {
    if (Objects.requireNonNull(docIds).isEmpty()) {
      throw new IllegalArgumentException("Empty document id list.");
    }
    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Set<ByteArray> terms = new HashSet<>();

    for (final Integer documentId : uniqueDocIds) {
      checkDocumentId(documentId);
      for (int fieldNum = 0; fieldNum < FIELD_COUNT; fieldNum++) {
        final Iterable<ByteArray> docTerms = Fun.filter(
            (NavigableSet<Fun.Tuple3<Integer, Integer, ByteArray>>)
                IDX.keySet(), fieldNum, documentId
        );
        for (final ByteArray docTerm : docTerms) {
          terms.add(docTerm);
        }
      }
    }
    return terms;
  }

  @Override
  public long getDocumentCount() {
    return DOC_COUNT;
  }

  @Override
  public boolean documentContains(final int documentId, final ByteArray term) {
    Objects.requireNonNull(term);
    checkDocumentId(documentId);

    for (int fieldNum = 0; fieldNum < FIELD_COUNT; fieldNum++) {
      final Iterable<ByteArray> docTerms = Fun.filter(
          (NavigableSet<Fun.Tuple3<Integer, Integer, ByteArray>>)
              IDX.keySet(), fieldNum, documentId
      );
      for (final ByteArray docTerm : docTerms) {
        if (term.equals(docTerm)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Long getLastIndexCommitGeneration() {
    return 0L;
  }

  @Override
  public Set<String> getDocumentFields() {
    return new HashSet<>(Arrays.asList(DOC_FIELDS));
  }

  @Override
  public Set<String> getStopwords() {
    return Collections.<String>emptySet();
  }

  @Override
  public boolean isDisposed() {
    return false;
  }
}
