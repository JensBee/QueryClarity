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
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.TempDiskIndex;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;

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
import java.util.Set;

/**
 * A fixed temporary Lucene index. It's purpose is to provide a static, well
 * known index to verify calculation results. <br> This {@link
 * IndexDataProvider} implementation does not support any switching of document
 * fields nor does it respect any stopwords (they will be ignored). <br> See
 * {@code fixedTestIndexContent.txt} and {@code processFixedTestIndexContent
 * .sh} in {@code src/test/resources/} for reference calculation sources.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("SpellCheckingInspection")
public final class FixedTestIndexDataProvider
    implements IndexDataProvider {

  /**
   * Temporary Lucene index held in memory.
   */
  public static final TempDiskIndex TMP_IDX;

  /**
   * Temporary directory to store calculation data in test runs. Created as
   * subdirectory of the temporary index directory.
   */
  public static final File DATA_DIR;
  /**
   * Number of fields per document.
   */
  static final int FIELD_COUNT = 3;
  /**
   * Number of documents in the index.
   */
  static final int DOC_COUNT = 10;
  /**
   * Document contents.
   */
  private static final List<String[]> DOCUMENTS;
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

    try {
      TMP_IDX = new TempDiskIndex(DOC_FIELDS);
    } catch (final IOException e) {
      throw new ExceptionInInitializerError(e);
    }

    DATA_DIR = new File(TMP_IDX.getIndexDir(), "data");
    if (!DATA_DIR.exists() && !DATA_DIR.mkdirs()) {
      throw new ExceptionInInitializerError(
          "Failed to create data directory: '" + DATA_DIR + "'.");
    }

    // create 10 "lorem ipsum" documents with 3 fields
    DOCUMENTS = new ArrayList<>(10);
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
    for (final String[] doc : DOCUMENTS) {
      try {
        TMP_IDX.addDoc(doc);
        TMP_IDX.flush();
      } catch (final IOException e) {
        throw new ExceptionInInitializerError(e);
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
   * Get some unique random terms from the index. The amount of terms returned
   * is the half of all index terms at maximum.
   *
   * @return Tuple containing a set of terms as String and {@link ByteArray}
   * @throws UnsupportedEncodingException Thrown, if a query term could not be
   * encoded to {@code UTF-8}
   */
  public static Tuple.Tuple2<Set<String>, Set<ByteArray>>
  getUniqueRandomIndexTerms()
      throws UnsupportedEncodingException {
    final Tuple.Tuple2<List<String>, List<ByteArray>> terms =
        getRandomIndexTerms();
    return Tuple.tuple2((Set<String>) new HashSet<>(terms.a),
        (Set<ByteArray>) new HashSet<>(terms.b));
  }

  /**
   * Get some non-unique random terms from the index. The amount of terms
   * returned is the half of all index terms at maximum.
   *
   * @return Tuple containing a set of terms as String and {@link ByteArray}
   * @throws UnsupportedEncodingException Thrown, if a query term could not be
   * encoded to {@code UTF-8}
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public static Tuple.Tuple2<List<String>,
      List<ByteArray>> getRandomIndexTerms()
      throws UnsupportedEncodingException {
    final int maxTerm = KnownData.IDX_TERMFREQ
        .size() - 1;
    int qTermCount = RandomValue.getInteger(0, maxTerm / 2);
    if (qTermCount == 0) {
      qTermCount = 1;
    }
    final List<ByteArray> qTerms = new ArrayList<>(qTermCount);
    final List<String> qTermsStr = new ArrayList<>(qTermCount);
    final List<String> idxTerms =
        new ArrayList<>(KnownData.IDX_TERMFREQ.keySet());

    for (int i = 0; i < qTermCount; i++) {
      final String term = idxTerms.get(RandomValue.getInteger(0, maxTerm));
      qTermsStr.add(term);
      qTerms.add(new ByteArray(term.getBytes("UTF-8")));
    }

    assert !qTerms.isEmpty();
    assert !qTermsStr.isEmpty();

    return Tuple.tuple2(qTermsStr, qTerms);
  }

  /**
   * Get a set of unique terms in index.
   *
   * @return Unique set of all index terms
   */
  public Set<ByteArray> getTermSet() {
    return getDocTermsSet(getDocIds());
  }

  private Set<ByteArray> getDocTermsSet(
      final Collection<Integer> docIds) {
    final Set<String> termsStr = getDocumentsTermSetStr(docIds);
    final Set<ByteArray> termsBa = new HashSet<>(termsStr.size());
    for (final String term : termsStr) {
      try {
        termsBa.add(new ByteArray(term.getBytes("UTF-8")));
      } catch (final UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
    }
    return termsBa;
  }

  private Set<Integer> getDocIds() {
    return getDocumentIdsSet();
  }

  @Override
  public Iterator<Integer> getDocumentIds() {
    return getDocumentIdsSet().iterator();
  }

  public Set<String> getDocumentsTermSetStr(
      final Collection<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      throw new IllegalArgumentException("Empty document id list.");
    }
    final Iterable<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Set<String> terms = new HashSet<>();

    for (final Integer documentId : uniqueDocIds) {
      checkDocumentId(documentId);
      for (final String term : KnownData.getDocumentTfMap(documentId)
          .keySet()) {
        terms.add(term);
      }
    }
    return terms;
  }

  /**
   * Get a unique set of all document ids.
   *
   * @return Document ids
   */
  public static Set<Integer> getDocumentIdsSet() {
    final Set<Integer> docIds = new HashSet<>(DOC_COUNT);
    for (int i = 0; i < DOC_COUNT; i++) {
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
    if (!hasDocument(docId)) {
      throw new IllegalArgumentException("Illegal document id: " + docId);
    }
  }

  /**
   * Get a sorted list (ascending) of all unique index terms.
   *
   * @return Sorted list of unique index terms (asc)
   */
  public List<String> getSortedTermList() {
    final List<String> termSet = new ArrayList<>(getTermSetStr());
    Collections.sort(termSet);
    return termSet;
  }

  /**
   * Get a set of unique terms in index.
   *
   * @return Unique set of all index terms
   */
  public Set<String> getTermSetStr() {
    return getDocumentsTermSetStr(getDocIds());
  }

  /**
   * Data dump of known (pre-calculated) values.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class KnownData {
    /**
     * Number of non-unique terms in index.
     */
    public static final int TERM_COUNT = 1000;
    /**
     * Number of unique terms in index.
     */
    public static final int TERM_COUNT_UNIQUE = 171;
    /**
     * Number of unique combinations of documents & terms. Used for sizing
     * storage maps.
     */
    public static final int DOC_TERM_PAIRS = 711;
    /**
     * Frequency values of all terms in index.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> IDX_TERMFREQ;
    /**
     * Document frequency values of all terms in index.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> IDX_DOCFREQ;
    /**
     * Term frequency values for document 0.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_0;
    /**
     * Number of documents in index.
     */
    public static final int DOC_COUNT = FixedTestIndexDataProvider.DOC_COUNT;
    static {
      final Map<String, Integer> idxTermFreq = new HashMap<>(171);
      idxTermFreq.put("et", 21);
      idxTermFreq.put("ut", 20);
      idxTermFreq.put("in", 20);
      idxTermFreq.put("nec", 18);
      idxTermFreq.put("vestibulum", 17);
      idxTermFreq.put("quis", 17);
      idxTermFreq.put("nunc", 17);
      idxTermFreq.put("sed", 15);
      idxTermFreq.put("ac", 15);
      idxTermFreq.put("eget", 13);
      idxTermFreq.put("a", 13);
      idxTermFreq.put("turpis", 12);
      idxTermFreq.put("phasellus", 12);
      idxTermFreq.put("mi", 12);
      idxTermFreq.put("libero", 12);
      idxTermFreq.put("pellentesque", 11);
      idxTermFreq.put("metus", 11);
      idxTermFreq.put("leo", 11);
      idxTermFreq.put("ipsum", 11);
      idxTermFreq.put("tincidunt", 10);
      idxTermFreq.put("posuere", 10);
      idxTermFreq.put("nisi", 10);
      idxTermFreq.put("id", 10);
      idxTermFreq.put("felis", 10);
      idxTermFreq.put("eu", 10);
      idxTermFreq.put("donec", 10);
      idxTermFreq.put("sit", 9);
      idxTermFreq.put("praesent", 9);
      idxTermFreq.put("orci", 9);
      idxTermFreq.put("ligula", 9);
      idxTermFreq.put("egestas", 9);
      idxTermFreq.put("dui", 9);
      idxTermFreq.put("dolor", 9);
      idxTermFreq.put("augue", 9);
      idxTermFreq.put("ante", 9);
      idxTermFreq.put("amet", 9);
      idxTermFreq.put("aenean", 9);
      idxTermFreq.put("velit", 8);
      idxTermFreq.put("vel", 8);
      idxTermFreq.put("quam", 8);
      idxTermFreq.put("pede", 8);
      idxTermFreq.put("non", 8);
      idxTermFreq.put("neque", 8);
      idxTermFreq.put("mauris", 8);
      idxTermFreq.put("lacus", 8);
      idxTermFreq.put("imperdiet", 8);
      idxTermFreq.put("fusce", 8);
      idxTermFreq.put("faucibus", 8);
      idxTermFreq.put("vitae", 7);
      idxTermFreq.put("purus", 7);
      idxTermFreq.put("pretium", 7);
      idxTermFreq.put("nullam", 7);
      idxTermFreq.put("nulla", 7);
      idxTermFreq.put("mollis", 7);
      idxTermFreq.put("malesuada", 7);
      idxTermFreq.put("lorem", 7);
      idxTermFreq.put("feugiat", 7);
      idxTermFreq.put("blandit", 7);
      idxTermFreq.put("at", 7);
      idxTermFreq.put("viverra", 6);
      idxTermFreq.put("ullamcorper", 6);
      idxTermFreq.put("tellus", 6);
      idxTermFreq.put("sem", 6);
      idxTermFreq.put("sapien", 6);
      idxTermFreq.put("sagittis", 6);
      idxTermFreq.put("morbi", 6);
      idxTermFreq.put("magna", 6);
      idxTermFreq.put("justo", 6);
      idxTermFreq.put("hendrerit", 6);
      idxTermFreq.put("fringilla", 6);
      idxTermFreq.put("etiam", 6);
      idxTermFreq.put("eros", 6);
      idxTermFreq.put("enim", 6);
      idxTermFreq.put("curabitur", 6);
      idxTermFreq.put("auctor", 6);
      idxTermFreq.put("arcu", 6);
      idxTermFreq.put("adipiscing", 6);
      idxTermFreq.put("vulputate", 5);
      idxTermFreq.put("volutpat", 5);
      idxTermFreq.put("varius", 5);
      idxTermFreq.put("ultricies", 5);
      idxTermFreq.put("tortor", 5);
      idxTermFreq.put("semper", 5);
      idxTermFreq.put("quisque", 5);
      idxTermFreq.put("nisl", 5);
      idxTermFreq.put("maecenas", 5);
      idxTermFreq.put("luctus", 5);
      idxTermFreq.put("iaculis", 5);
      idxTermFreq.put("eleifend", 5);
      idxTermFreq.put("cursus", 5);
      idxTermFreq.put("consectetuer", 5);
      idxTermFreq.put("condimentum", 5);
      idxTermFreq.put("aliquam", 5);
      idxTermFreq.put("venenatis", 4);
      idxTermFreq.put("urna", 4);
      idxTermFreq.put("ultrices", 4);
      idxTermFreq.put("sodales", 4);
      idxTermFreq.put("rutrum", 4);
      idxTermFreq.put("risus", 4);
      idxTermFreq.put("rhoncus", 4);
      idxTermFreq.put("pulvinar", 4);
      idxTermFreq.put("placerat", 4);
      idxTermFreq.put("odio", 4);
      idxTermFreq.put("nonummy", 4);
      idxTermFreq.put("nibh", 4);
      idxTermFreq.put("nam", 4);
      idxTermFreq.put("massa", 4);
      idxTermFreq.put("laoreet", 4);
      idxTermFreq.put("euismod", 4);
      idxTermFreq.put("erat", 4);
      idxTermFreq.put("elit", 4);
      idxTermFreq.put("cras", 4);
      idxTermFreq.put("accumsan", 4);
      idxTermFreq.put("tristique", 3);
      idxTermFreq.put("tempus", 3);
      idxTermFreq.put("tempor", 3);
      idxTermFreq.put("suspendisse", 3);
      idxTermFreq.put("primis", 3);
      idxTermFreq.put("molestie", 3);
      idxTermFreq.put("mattis", 3);
      idxTermFreq.put("gravida", 3);
      idxTermFreq.put("fermentum", 3);
      idxTermFreq.put("est", 3);
      idxTermFreq.put("elementum", 3);
      idxTermFreq.put("dapibus", 3);
      idxTermFreq.put("curae", 3);
      idxTermFreq.put("cubilia", 3);
      idxTermFreq.put("convallis", 3);
      idxTermFreq.put("consequat", 3);
      idxTermFreq.put("congue", 3);
      idxTermFreq.put("vivamus", 2);
      idxTermFreq.put("vehicula", 2);
      idxTermFreq.put("suscipit", 2);
      idxTermFreq.put("sollicitudin", 2);
      idxTermFreq.put("senectus", 2);
      idxTermFreq.put("scelerisque", 2);
      idxTermFreq.put("proin", 2);
      idxTermFreq.put("porttitor", 2);
      idxTermFreq.put("porta", 2);
      idxTermFreq.put("platea", 2);
      idxTermFreq.put("netus", 2);
      idxTermFreq.put("lobortis", 2);
      idxTermFreq.put("lectus", 2);
      idxTermFreq.put("lacinia", 2);
      idxTermFreq.put("interdum", 2);
      idxTermFreq.put("integer", 2);
      idxTermFreq.put("hac", 2);
      idxTermFreq.put("habitasse", 2);
      idxTermFreq.put("habitant", 2);
      idxTermFreq.put("fames", 2);
      idxTermFreq.put("facilisis", 2);
      idxTermFreq.put("duis", 2);
      idxTermFreq.put("dictumst", 2);
      idxTermFreq.put("dictum", 2);
      idxTermFreq.put("diam", 2);
      idxTermFreq.put("commodo", 2);
      idxTermFreq.put("bibendum", 2);
      idxTermFreq.put("aliquet", 2);
      idxTermFreq.put("sociis", 1);
      idxTermFreq.put("ridiculus", 1);
      idxTermFreq.put("pharetra", 1);
      idxTermFreq.put("penatibus", 1);
      idxTermFreq.put("parturient", 1);
      idxTermFreq.put("ornare", 1);
      idxTermFreq.put("natoque", 1);
      idxTermFreq.put("nascetur", 1);
      idxTermFreq.put("mus", 1);
      idxTermFreq.put("montes", 1);
      idxTermFreq.put("magnis", 1);
      idxTermFreq.put("dis", 1);
      idxTermFreq.put("cum", 1);
      IDX_TERMFREQ = Collections.unmodifiableMap(idxTermFreq);
    }

    static {
      final Map<String, Integer> idxDocFreq = new HashMap<>(171);
      idxDocFreq.put("a", 9);
      idxDocFreq.put("ac", 7);
      idxDocFreq.put("accumsan", 4);
      idxDocFreq.put("adipiscing", 6);
      idxDocFreq.put("aenean", 6);
      idxDocFreq.put("aliquam", 4);
      idxDocFreq.put("aliquet", 2);
      idxDocFreq.put("amet", 7);
      idxDocFreq.put("ante", 6);
      idxDocFreq.put("arcu", 5);
      idxDocFreq.put("at", 4);
      idxDocFreq.put("auctor", 5);
      idxDocFreq.put("augue", 6);
      idxDocFreq.put("bibendum", 2);
      idxDocFreq.put("blandit", 4);
      idxDocFreq.put("commodo", 2);
      idxDocFreq.put("condimentum", 4);
      idxDocFreq.put("congue", 2);
      idxDocFreq.put("consectetuer", 5);
      idxDocFreq.put("consequat", 3);
      idxDocFreq.put("convallis", 3);
      idxDocFreq.put("cras", 4);
      idxDocFreq.put("cubilia", 3);
      idxDocFreq.put("cum", 1);
      idxDocFreq.put("curabitur", 4);
      idxDocFreq.put("curae", 3);
      idxDocFreq.put("cursus", 4);
      idxDocFreq.put("dapibus", 3);
      idxDocFreq.put("diam", 2);
      idxDocFreq.put("dictum", 2);
      idxDocFreq.put("dictumst", 2);
      idxDocFreq.put("dis", 1);
      idxDocFreq.put("dolor", 7);
      idxDocFreq.put("donec", 8);
      idxDocFreq.put("dui", 7);
      idxDocFreq.put("duis", 1);
      idxDocFreq.put("egestas", 5);
      idxDocFreq.put("eget", 8);
      idxDocFreq.put("eleifend", 5);
      idxDocFreq.put("elementum", 2);
      idxDocFreq.put("elit", 3);
      idxDocFreq.put("enim", 5);
      idxDocFreq.put("erat", 3);
      idxDocFreq.put("eros", 5);
      idxDocFreq.put("est", 3);
      idxDocFreq.put("et", 10);
      idxDocFreq.put("etiam", 3);
      idxDocFreq.put("eu", 7);
      idxDocFreq.put("euismod", 4);
      idxDocFreq.put("facilisis", 2);
      idxDocFreq.put("fames", 2);
      idxDocFreq.put("faucibus", 5);
      idxDocFreq.put("felis", 3);
      idxDocFreq.put("fermentum", 2);
      idxDocFreq.put("feugiat", 7);
      idxDocFreq.put("fringilla", 5);
      idxDocFreq.put("fusce", 5);
      idxDocFreq.put("gravida", 3);
      idxDocFreq.put("habitant", 2);
      idxDocFreq.put("habitasse", 2);
      idxDocFreq.put("hac", 2);
      idxDocFreq.put("hendrerit", 5);
      idxDocFreq.put("iaculis", 3);
      idxDocFreq.put("id", 7);
      idxDocFreq.put("imperdiet", 4);
      idxDocFreq.put("in", 10);
      idxDocFreq.put("integer", 2);
      idxDocFreq.put("interdum", 2);
      idxDocFreq.put("ipsum", 9);
      idxDocFreq.put("justo", 4);
      idxDocFreq.put("lacinia", 2);
      idxDocFreq.put("lacus", 5);
      idxDocFreq.put("laoreet", 4);
      idxDocFreq.put("lectus", 2);
      idxDocFreq.put("leo", 7);
      idxDocFreq.put("libero", 8);
      idxDocFreq.put("ligula", 6);
      idxDocFreq.put("lobortis", 2);
      idxDocFreq.put("lorem", 5);
      idxDocFreq.put("luctus", 5);
      idxDocFreq.put("maecenas", 4);
      idxDocFreq.put("magna", 4);
      idxDocFreq.put("magnis", 1);
      idxDocFreq.put("malesuada", 4);
      idxDocFreq.put("massa", 3);
      idxDocFreq.put("mattis", 3);
      idxDocFreq.put("mauris", 5);
      idxDocFreq.put("metus", 7);
      idxDocFreq.put("mi", 5);
      idxDocFreq.put("molestie", 2);
      idxDocFreq.put("mollis", 6);
      idxDocFreq.put("montes", 1);
      idxDocFreq.put("morbi", 4);
      idxDocFreq.put("mus", 1);
      idxDocFreq.put("nam", 3);
      idxDocFreq.put("nascetur", 1);
      idxDocFreq.put("natoque", 1);
      idxDocFreq.put("nec", 9);
      idxDocFreq.put("neque", 6);
      idxDocFreq.put("netus", 2);
      idxDocFreq.put("nibh", 4);
      idxDocFreq.put("nisi", 8);
      idxDocFreq.put("nisl", 4);
      idxDocFreq.put("non", 6);
      idxDocFreq.put("nonummy", 3);
      idxDocFreq.put("nulla", 6);
      idxDocFreq.put("nullam", 7);
      idxDocFreq.put("nunc", 9);
      idxDocFreq.put("odio", 4);
      idxDocFreq.put("orci", 6);
      idxDocFreq.put("ornare", 1);
      idxDocFreq.put("parturient", 1);
      idxDocFreq.put("pede", 6);
      idxDocFreq.put("pellentesque", 6);
      idxDocFreq.put("penatibus", 1);
      idxDocFreq.put("pharetra", 1);
      idxDocFreq.put("phasellus", 7);
      idxDocFreq.put("placerat", 4);
      idxDocFreq.put("platea", 2);
      idxDocFreq.put("porta", 2);
      idxDocFreq.put("porttitor", 2);
      idxDocFreq.put("posuere", 4);
      idxDocFreq.put("praesent", 5);
      idxDocFreq.put("pretium", 5);
      idxDocFreq.put("primis", 3);
      idxDocFreq.put("proin", 2);
      idxDocFreq.put("pulvinar", 3);
      idxDocFreq.put("purus", 5);
      idxDocFreq.put("quam", 6);
      idxDocFreq.put("quis", 8);
      idxDocFreq.put("quisque", 4);
      idxDocFreq.put("rhoncus", 3);
      idxDocFreq.put("ridiculus", 1);
      idxDocFreq.put("risus", 4);
      idxDocFreq.put("rutrum", 4);
      idxDocFreq.put("sagittis", 5);
      idxDocFreq.put("sapien", 6);
      idxDocFreq.put("scelerisque", 2);
      idxDocFreq.put("sed", 9);
      idxDocFreq.put("sem", 5);
      idxDocFreq.put("semper", 4);
      idxDocFreq.put("senectus", 2);
      idxDocFreq.put("sit", 7);
      idxDocFreq.put("sociis", 1);
      idxDocFreq.put("sodales", 3);
      idxDocFreq.put("sollicitudin", 2);
      idxDocFreq.put("suscipit", 2);
      idxDocFreq.put("suspendisse", 2);
      idxDocFreq.put("tellus", 4);
      idxDocFreq.put("tempor", 2);
      idxDocFreq.put("tempus", 2);
      idxDocFreq.put("tincidunt", 7);
      idxDocFreq.put("tortor", 4);
      idxDocFreq.put("tristique", 2);
      idxDocFreq.put("turpis", 6);
      idxDocFreq.put("ullamcorper", 5);
      idxDocFreq.put("ultrices", 4);
      idxDocFreq.put("ultricies", 4);
      idxDocFreq.put("urna", 4);
      idxDocFreq.put("ut", 9);
      idxDocFreq.put("varius", 4);
      idxDocFreq.put("vehicula", 2);
      idxDocFreq.put("vel", 6);
      idxDocFreq.put("velit", 5);
      idxDocFreq.put("venenatis", 4);
      idxDocFreq.put("vestibulum", 7);
      idxDocFreq.put("vitae", 5);
      idxDocFreq.put("vivamus", 2);
      idxDocFreq.put("viverra", 4);
      idxDocFreq.put("volutpat", 3);
      idxDocFreq.put("vulputate", 4);
      IDX_DOCFREQ = Collections.unmodifiableMap(idxDocFreq);
    }
    /**
     * Term frequency values for document 1.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_1;
    static {
      final Map<String, Integer> tfDoc1 = new HashMap<>(70);
      tfDoc1.put("etiam", 3);
      tfDoc1.put("eget", 3);
      tfDoc1.put("ante", 3);
      tfDoc1.put("viverra", 2);
      tfDoc1.put("vitae", 2);
      tfDoc1.put("vel", 2);
      tfDoc1.put("ut", 2);
      tfDoc1.put("ultricies", 2);
      tfDoc1.put("tincidunt", 2);
      tfDoc1.put("tempus", 2);
      tfDoc1.put("tellus", 2);
      tfDoc1.put("sit", 2);
      tfDoc1.put("sem", 2);
      tfDoc1.put("rhoncus", 2);
      tfDoc1.put("quis", 2);
      tfDoc1.put("quam", 2);
      tfDoc1.put("nisi", 2);
      tfDoc1.put("nam", 2);
      tfDoc1.put("maecenas", 2);
      tfDoc1.put("lorem", 2);
      tfDoc1.put("libero", 2);
      tfDoc1.put("faucibus", 2);
      tfDoc1.put("amet", 2);
      tfDoc1.put("aenean", 2);
      tfDoc1.put("venenatis", 1);
      tfDoc1.put("varius", 1);
      tfDoc1.put("ullamcorper", 1);
      tfDoc1.put("semper", 1);
      tfDoc1.put("sed", 1);
      tfDoc1.put("sapien", 1);
      tfDoc1.put("rutrum", 1);
      tfDoc1.put("quisque", 1);
      tfDoc1.put("pulvinar", 1);
      tfDoc1.put("porttitor", 1);
      tfDoc1.put("phasellus", 1);
      tfDoc1.put("orci", 1);
      tfDoc1.put("odio", 1);
      tfDoc1.put("nunc", 1);
      tfDoc1.put("nullam", 1);
      tfDoc1.put("nulla", 1);
      tfDoc1.put("neque", 1);
      tfDoc1.put("nec", 1);
      tfDoc1.put("metus", 1);
      tfDoc1.put("luctus", 1);
      tfDoc1.put("ligula", 1);
      tfDoc1.put("leo", 1);
      tfDoc1.put("laoreet", 1);
      tfDoc1.put("ipsum", 1);
      tfDoc1.put("in", 1);
      tfDoc1.put("imperdiet", 1);
      tfDoc1.put("id", 1);
      tfDoc1.put("hendrerit", 1);
      tfDoc1.put("feugiat", 1);
      tfDoc1.put("eu", 1);
      tfDoc1.put("et", 1);
      tfDoc1.put("eros", 1);
      tfDoc1.put("enim", 1);
      tfDoc1.put("eleifend", 1);
      tfDoc1.put("dui", 1);
      tfDoc1.put("donec", 1);
      tfDoc1.put("dapibus", 1);
      tfDoc1.put("curabitur", 1);
      tfDoc1.put("consequat", 1);
      tfDoc1.put("condimentum", 1);
      tfDoc1.put("blandit", 1);
      tfDoc1.put("augue", 1);
      tfDoc1.put("aliquam", 1);
      tfDoc1.put("adipiscing", 1);
      tfDoc1.put("ac", 1);
      tfDoc1.put("a", 1);
      TF_DOC_1 = Collections.unmodifiableMap(tfDoc1);
    }
    /**
     * Term frequency values for document 2.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_2;
    static {
      final Map<String, Integer> tfDoc0 = new HashMap<>(65);
      tfDoc0.put("justo", 3);
      tfDoc0.put("aenean", 3);
      tfDoc0.put("vulputate", 2);
      tfDoc0.put("quis", 2);
      tfDoc0.put("pretium", 2);
      tfDoc0.put("pede", 2);
      tfDoc0.put("nec", 2);
      tfDoc0.put("massa", 2);
      tfDoc0.put("felis", 2);
      tfDoc0.put("eu", 2);
      tfDoc0.put("enim", 2);
      tfDoc0.put("eget", 2);
      tfDoc0.put("donec", 2);
      tfDoc0.put("dolor", 2);
      tfDoc0.put("vivamus", 1);
      tfDoc0.put("vitae", 1);
      tfDoc0.put("venenatis", 1);
      tfDoc0.put("vel", 1);
      tfDoc0.put("ut", 1);
      tfDoc0.put("ultricies", 1);
      tfDoc0.put("tincidunt", 1);
      tfDoc0.put("tellus", 1);
      tfDoc0.put("sociis", 1);
      tfDoc0.put("sit", 1);
      tfDoc0.put("semper", 1);
      tfDoc0.put("sem", 1);
      tfDoc0.put("ridiculus", 1);
      tfDoc0.put("rhoncus", 1);
      tfDoc0.put("quam", 1);
      tfDoc0.put("penatibus", 1);
      tfDoc0.put("pellentesque", 1);
      tfDoc0.put("parturient", 1);
      tfDoc0.put("nullam", 1);
      tfDoc0.put("nulla", 1);
      tfDoc0.put("nisi", 1);
      tfDoc0.put("natoque", 1);
      tfDoc0.put("nascetur", 1);
      tfDoc0.put("mus", 1);
      tfDoc0.put("montes", 1);
      tfDoc0.put("mollis", 1);
      tfDoc0.put("magnis", 1);
      tfDoc0.put("lorem", 1);
      tfDoc0.put("ligula", 1);
      tfDoc0.put("ipsum", 1);
      tfDoc0.put("integer", 1);
      tfDoc0.put("in", 1);
      tfDoc0.put("imperdiet", 1);
      tfDoc0.put("fringilla", 1);
      tfDoc0.put("et", 1);
      tfDoc0.put("elit", 1);
      tfDoc0.put("elementum", 1);
      tfDoc0.put("eleifend", 1);
      tfDoc0.put("dis", 1);
      tfDoc0.put("dictum", 1);
      tfDoc0.put("dapibus", 1);
      tfDoc0.put("cum", 1);
      tfDoc0.put("cras", 1);
      tfDoc0.put("consequat", 1);
      tfDoc0.put("consectetuer", 1);
      tfDoc0.put("commodo", 1);
      tfDoc0.put("arcu", 1);
      tfDoc0.put("amet", 1);
      tfDoc0.put("aliquet", 1);
      tfDoc0.put("adipiscing", 1);
      tfDoc0.put("a", 1);
      TF_DOC_0 = Collections.unmodifiableMap(tfDoc0);
    }

    static {
      final Map<String, Integer> tfDoc2 = new HashMap<>(69);
      tfDoc2.put("sed", 3);
      tfDoc2.put("mi", 3);
      tfDoc2.put("in", 3);
      tfDoc2.put("vestibulum", 2);
      tfDoc2.put("turpis", 2);
      tfDoc2.put("sodales", 2);
      tfDoc2.put("quis", 2);
      tfDoc2.put("magna", 2);
      tfDoc2.put("leo", 2);
      tfDoc2.put("ipsum", 2);
      tfDoc2.put("imperdiet", 2);
      tfDoc2.put("fringilla", 2);
      tfDoc2.put("et", 2);
      tfDoc2.put("eget", 2);
      tfDoc2.put("duis", 2);
      tfDoc2.put("dui", 2);
      tfDoc2.put("arcu", 2);
      tfDoc2.put("vulputate", 1);
      tfDoc2.put("velit", 1);
      tfDoc2.put("ut", 1);
      tfDoc2.put("ultricies", 1);
      tfDoc2.put("ultrices", 1);
      tfDoc2.put("tortor", 1);
      tfDoc2.put("suscipit", 1);
      tfDoc2.put("sit", 1);
      tfDoc2.put("scelerisque", 1);
      tfDoc2.put("sapien", 1);
      tfDoc2.put("sagittis", 1);
      tfDoc2.put("quam", 1);
      tfDoc2.put("purus", 1);
      tfDoc2.put("primis", 1);
      tfDoc2.put("pretium", 1);
      tfDoc2.put("posuere", 1);
      tfDoc2.put("orci", 1);
      tfDoc2.put("nunc", 1);
      tfDoc2.put("nullam", 1);
      tfDoc2.put("nonummy", 1);
      tfDoc2.put("nibh", 1);
      tfDoc2.put("nec", 1);
      tfDoc2.put("nam", 1);
      tfDoc2.put("mollis", 1);
      tfDoc2.put("metus", 1);
      tfDoc2.put("mauris", 1);
      tfDoc2.put("luctus", 1);
      tfDoc2.put("lorem", 1);
      tfDoc2.put("libero", 1);
      tfDoc2.put("lacinia", 1);
      tfDoc2.put("id", 1);
      tfDoc2.put("iaculis", 1);
      tfDoc2.put("hendrerit", 1);
      tfDoc2.put("gravida", 1);
      tfDoc2.put("fusce", 1);
      tfDoc2.put("faucibus", 1);
      tfDoc2.put("eu", 1);
      tfDoc2.put("eleifend", 1);
      tfDoc2.put("donec", 1);
      tfDoc2.put("cursus", 1);
      tfDoc2.put("curae", 1);
      tfDoc2.put("cubilia", 1);
      tfDoc2.put("cras", 1);
      tfDoc2.put("consequat", 1);
      tfDoc2.put("consectetuer", 1);
      tfDoc2.put("bibendum", 1);
      tfDoc2.put("augue", 1);
      tfDoc2.put("ante", 1);
      tfDoc2.put("amet", 1);
      tfDoc2.put("accumsan", 1);
      tfDoc2.put("ac", 1);
      tfDoc2.put("a", 1);
      TF_DOC_2 = Collections.unmodifiableMap(tfDoc2);
    }

    /**
     * Number of fields per document.
     */
    public static final int FIELD_COUNT = FixedTestIndexDataProvider
        .FIELD_COUNT;
    /**
     * Term frequency values for document 3.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_3;
    static {
      final Map<String, Integer> tfDoc3 = new HashMap<>(71);
      tfDoc3.put("imperdiet", 4);
      tfDoc3.put("phasellus", 3);
      tfDoc3.put("nunc", 3);
      tfDoc3.put("vestibulum", 2);
      tfDoc3.put("ut", 2);
      tfDoc3.put("sed", 2);
      tfDoc3.put("posuere", 2);
      tfDoc3.put("nonummy", 2);
      tfDoc3.put("non", 2);
      tfDoc3.put("nec", 2);
      tfDoc3.put("mauris", 2);
      tfDoc3.put("leo", 2);
      tfDoc3.put("id", 2);
      tfDoc3.put("hendrerit", 2);
      tfDoc3.put("et", 2);
      tfDoc3.put("eros", 2);
      tfDoc3.put("volutpat", 1);
      tfDoc3.put("vitae", 1);
      tfDoc3.put("ultricies", 1);
      tfDoc3.put("ultrices", 1);
      tfDoc3.put("ullamcorper", 1);
      tfDoc3.put("tincidunt", 1);
      tfDoc3.put("tempus", 1);
      tfDoc3.put("sit", 1);
      tfDoc3.put("sem", 1);
      tfDoc3.put("sapien", 1);
      tfDoc3.put("sagittis", 1);
      tfDoc3.put("rutrum", 1);
      tfDoc3.put("risus", 1);
      tfDoc3.put("quis", 1);
      tfDoc3.put("pretium", 1);
      tfDoc3.put("praesent", 1);
      tfDoc3.put("pellentesque", 1);
      tfDoc3.put("pede", 1);
      tfDoc3.put("orci", 1);
      tfDoc3.put("nullam", 1);
      tfDoc3.put("nulla", 1);
      tfDoc3.put("nisl", 1);
      tfDoc3.put("nisi", 1);
      tfDoc3.put("neque", 1);
      tfDoc3.put("mollis", 1);
      tfDoc3.put("metus", 1);
      tfDoc3.put("malesuada", 1);
      tfDoc3.put("maecenas", 1);
      tfDoc3.put("ligula", 1);
      tfDoc3.put("libero", 1);
      tfDoc3.put("lectus", 1);
      tfDoc3.put("justo", 1);
      tfDoc3.put("ipsum", 1);
      tfDoc3.put("integer", 1);
      tfDoc3.put("in", 1);
      tfDoc3.put("feugiat", 1);
      tfDoc3.put("facilisis", 1);
      tfDoc3.put("euismod", 1);
      tfDoc3.put("etiam", 1);
      tfDoc3.put("eget", 1);
      tfDoc3.put("dui", 1);
      tfDoc3.put("donec", 1);
      tfDoc3.put("dolor", 1);
      tfDoc3.put("curabitur", 1);
      tfDoc3.put("cras", 1);
      tfDoc3.put("consectetuer", 1);
      tfDoc3.put("auctor", 1);
      tfDoc3.put("arcu", 1);
      tfDoc3.put("ante", 1);
      tfDoc3.put("amet", 1);
      tfDoc3.put("aliquam", 1);
      tfDoc3.put("aenean", 1);
      tfDoc3.put("adipiscing", 1);
      tfDoc3.put("accumsan", 1);
      tfDoc3.put("a", 1);
      TF_DOC_3 = Collections.unmodifiableMap(tfDoc3);
    }

    /**
     * Term frequency values for document 4.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_4;

    static {
      final Map<String, Integer> tfDoc4 = new HashMap<>(53);
      tfDoc4.put("posuere", 4);
      tfDoc4.put("vestibulum", 3);
      tfDoc4.put("turpis", 3);
      tfDoc4.put("sed", 3);
      tfDoc4.put("orci", 3);
      tfDoc4.put("quis", 2);
      tfDoc4.put("praesent", 2);
      tfDoc4.put("phasellus", 2);
      tfDoc4.put("in", 2);
      tfDoc4.put("erat", 2);
      tfDoc4.put("elit", 2);
      tfDoc4.put("cursus", 2);
      tfDoc4.put("congue", 2);
      tfDoc4.put("ac", 2);
      tfDoc4.put("vulputate", 1);
      tfDoc4.put("vitae", 1);
      tfDoc4.put("velit", 1);
      tfDoc4.put("ultrices", 1);
      tfDoc4.put("tortor", 1);
      tfDoc4.put("tellus", 1);
      tfDoc4.put("sit", 1);
      tfDoc4.put("primis", 1);
      tfDoc4.put("porttitor", 1);
      tfDoc4.put("placerat", 1);
      tfDoc4.put("pellentesque", 1);
      tfDoc4.put("pede", 1);
      tfDoc4.put("nunc", 1);
      tfDoc4.put("non", 1);
      tfDoc4.put("nisi", 1);
      tfDoc4.put("metus", 1);
      tfDoc4.put("mattis", 1);
      tfDoc4.put("massa", 1);
      tfDoc4.put("luctus", 1);
      tfDoc4.put("lectus", 1);
      tfDoc4.put("ipsum", 1);
      tfDoc4.put("fringilla", 1);
      tfDoc4.put("faucibus", 1);
      tfDoc4.put("euismod", 1);
      tfDoc4.put("et", 1);
      tfDoc4.put("donec", 1);
      tfDoc4.put("dolor", 1);
      tfDoc4.put("curae", 1);
      tfDoc4.put("cubilia", 1);
      tfDoc4.put("consectetuer", 1);
      tfDoc4.put("bibendum", 1);
      tfDoc4.put("augue", 1);
      tfDoc4.put("at", 1);
      tfDoc4.put("arcu", 1);
      tfDoc4.put("ante", 1);
      tfDoc4.put("amet", 1);
      tfDoc4.put("aliquam", 1);
      tfDoc4.put("aenean", 1);
      tfDoc4.put("accumsan", 1);
      TF_DOC_4 = Collections.unmodifiableMap(tfDoc4);
    }

    /**
     * Term frequency values for document 5.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_5;

    static {
      final Map<String, Integer> tfDoc5 = new HashMap<>(74);
      tfDoc5.put("et", 4);
      tfDoc5.put("vestibulum", 3);
      tfDoc5.put("posuere", 3);
      tfDoc5.put("pellentesque", 3);
      tfDoc5.put("nunc", 3);
      tfDoc5.put("libero", 3);
      tfDoc5.put("volutpat", 2);
      tfDoc5.put("ut", 2);
      tfDoc5.put("turpis", 2);
      tfDoc5.put("sagittis", 2);
      tfDoc5.put("nec", 2);
      tfDoc5.put("in", 2);
      tfDoc5.put("dolor", 2);
      tfDoc5.put("augue", 2);
      tfDoc5.put("ac", 2);
      tfDoc5.put("venenatis", 1);
      tfDoc5.put("velit", 1);
      tfDoc5.put("varius", 1);
      tfDoc5.put("urna", 1);
      tfDoc5.put("ultrices", 1);
      tfDoc5.put("tristique", 1);
      tfDoc5.put("tortor", 1);
      tfDoc5.put("tincidunt", 1);
      tfDoc5.put("tempor", 1);
      tfDoc5.put("suspendisse", 1);
      tfDoc5.put("suscipit", 1);
      tfDoc5.put("sollicitudin", 1);
      tfDoc5.put("sodales", 1);
      tfDoc5.put("senectus", 1);
      tfDoc5.put("sem", 1);
      tfDoc5.put("sed", 1);
      tfDoc5.put("quis", 1);
      tfDoc5.put("purus", 1);
      tfDoc5.put("pulvinar", 1);
      tfDoc5.put("primis", 1);
      tfDoc5.put("phasellus", 1);
      tfDoc5.put("pede", 1);
      tfDoc5.put("orci", 1);
      tfDoc5.put("nullam", 1);
      tfDoc5.put("non", 1);
      tfDoc5.put("nibh", 1);
      tfDoc5.put("netus", 1);
      tfDoc5.put("neque", 1);
      tfDoc5.put("morbi", 1);
      tfDoc5.put("mollis", 1);
      tfDoc5.put("malesuada", 1);
      tfDoc5.put("magna", 1);
      tfDoc5.put("maecenas", 1);
      tfDoc5.put("luctus", 1);
      tfDoc5.put("lacus", 1);
      tfDoc5.put("justo", 1);
      tfDoc5.put("ipsum", 1);
      tfDoc5.put("id", 1);
      tfDoc5.put("habitant", 1);
      tfDoc5.put("fusce", 1);
      tfDoc5.put("feugiat", 1);
      tfDoc5.put("faucibus", 1);
      tfDoc5.put("fames", 1);
      tfDoc5.put("eu", 1);
      tfDoc5.put("elit", 1);
      tfDoc5.put("eget", 1);
      tfDoc5.put("egestas", 1);
      tfDoc5.put("dui", 1);
      tfDoc5.put("donec", 1);
      tfDoc5.put("diam", 1);
      tfDoc5.put("cursus", 1);
      tfDoc5.put("curae", 1);
      tfDoc5.put("cubilia", 1);
      tfDoc5.put("condimentum", 1);
      tfDoc5.put("blandit", 1);
      tfDoc5.put("auctor", 1);
      tfDoc5.put("ante", 1);
      tfDoc5.put("aenean", 1);
      tfDoc5.put("a", 1);
      TF_DOC_5 = Collections.unmodifiableMap(tfDoc5);
    }

    /**
     * Term frequency values for document 6.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_6;

    static {
      final Map<String, Integer> tfDoc6 = new HashMap<>(72);
      tfDoc6.put("felis", 5);
      tfDoc6.put("quis", 4);
      tfDoc6.put("pellentesque", 4);
      tfDoc6.put("ac", 4);
      tfDoc6.put("velit", 3);
      tfDoc6.put("phasellus", 3);
      tfDoc6.put("morbi", 3);
      tfDoc6.put("malesuada", 3);
      tfDoc6.put("in", 3);
      tfDoc6.put("et", 3);
      tfDoc6.put("at", 3);
      tfDoc6.put("a", 3);
      tfDoc6.put("ut", 2);
      tfDoc6.put("tristique", 2);
      tfDoc6.put("tortor", 2);
      tfDoc6.put("tincidunt", 2);
      tfDoc6.put("semper", 2);
      tfDoc6.put("pretium", 2);
      tfDoc6.put("pede", 2);
      tfDoc6.put("nunc", 2);
      tfDoc6.put("nec", 2);
      tfDoc6.put("mi", 2);
      tfDoc6.put("leo", 2);
      tfDoc6.put("egestas", 2);
      tfDoc6.put("curabitur", 2);
      tfDoc6.put("auctor", 2);
      tfDoc6.put("viverra", 1);
      tfDoc6.put("vivamus", 1);
      tfDoc6.put("vel", 1);
      tfDoc6.put("vehicula", 1);
      tfDoc6.put("urna", 1);
      tfDoc6.put("ullamcorper", 1);
      tfDoc6.put("turpis", 1);
      tfDoc6.put("senectus", 1);
      tfDoc6.put("sem", 1);
      tfDoc6.put("sed", 1);
      tfDoc6.put("sapien", 1);
      tfDoc6.put("rhoncus", 1);
      tfDoc6.put("quam", 1);
      tfDoc6.put("proin", 1);
      tfDoc6.put("porta", 1);
      tfDoc6.put("platea", 1);
      tfDoc6.put("ornare", 1);
      tfDoc6.put("nullam", 1);
      tfDoc6.put("non", 1);
      tfDoc6.put("nisi", 1);
      tfDoc6.put("netus", 1);
      tfDoc6.put("neque", 1);
      tfDoc6.put("mattis", 1);
      tfDoc6.put("magna", 1);
      tfDoc6.put("lobortis", 1);
      tfDoc6.put("libero", 1);
      tfDoc6.put("laoreet", 1);
      tfDoc6.put("lacus", 1);
      tfDoc6.put("ipsum", 1);
      tfDoc6.put("hendrerit", 1);
      tfDoc6.put("hac", 1);
      tfDoc6.put("habitasse", 1);
      tfDoc6.put("habitant", 1);
      tfDoc6.put("gravida", 1);
      tfDoc6.put("fringilla", 1);
      tfDoc6.put("feugiat", 1);
      tfDoc6.put("fames", 1);
      tfDoc6.put("euismod", 1);
      tfDoc6.put("est", 1);
      tfDoc6.put("eros", 1);
      tfDoc6.put("enim", 1);
      tfDoc6.put("eleifend", 1);
      tfDoc6.put("eget", 1);
      tfDoc6.put("dictumst", 1);
      tfDoc6.put("augue", 1);
      tfDoc6.put("aenean", 1);
      TF_DOC_6 = Collections.unmodifiableMap(tfDoc6);
    }

    /**
     * Term frequency values for document 7.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_7;

    static {
      final Map<String, Integer> tfDoc7 = new HashMap<>(74);
      tfDoc7.put("et", 5);
      tfDoc7.put("metus", 4);
      tfDoc7.put("fusce", 3);
      tfDoc7.put("felis", 3);
      tfDoc7.put("viverra", 2);
      tfDoc7.put("varius", 2);
      tfDoc7.put("ut", 2);
      tfDoc7.put("ullamcorper", 2);
      tfDoc7.put("tempor", 2);
      tfDoc7.put("nunc", 2);
      tfDoc7.put("nec", 2);
      tfDoc7.put("magna", 2);
      tfDoc7.put("lorem", 2);
      tfDoc7.put("ligula", 2);
      tfDoc7.put("libero", 2);
      tfDoc7.put("in", 2);
      tfDoc7.put("fermentum", 2);
      tfDoc7.put("eu", 2);
      tfDoc7.put("donec", 2);
      tfDoc7.put("condimentum", 2);
      tfDoc7.put("blandit", 2);
      tfDoc7.put("vulputate", 1);
      tfDoc7.put("vestibulum", 1);
      tfDoc7.put("venenatis", 1);
      tfDoc7.put("vel", 1);
      tfDoc7.put("turpis", 1);
      tfDoc7.put("tincidunt", 1);
      tfDoc7.put("sed", 1);
      tfDoc7.put("sapien", 1);
      tfDoc7.put("sagittis", 1);
      tfDoc7.put("risus", 1);
      tfDoc7.put("quisque", 1);
      tfDoc7.put("purus", 1);
      tfDoc7.put("pretium", 1);
      tfDoc7.put("praesent", 1);
      tfDoc7.put("placerat", 1);
      tfDoc7.put("phasellus", 1);
      tfDoc7.put("pede", 1);
      tfDoc7.put("odio", 1);
      tfDoc7.put("nullam", 1);
      tfDoc7.put("nulla", 1);
      tfDoc7.put("nisl", 1);
      tfDoc7.put("nisi", 1);
      tfDoc7.put("nibh", 1);
      tfDoc7.put("morbi", 1);
      tfDoc7.put("mollis", 1);
      tfDoc7.put("mi", 1);
      tfDoc7.put("mauris", 1);
      tfDoc7.put("maecenas", 1);
      tfDoc7.put("luctus", 1);
      tfDoc7.put("lobortis", 1);
      tfDoc7.put("leo", 1);
      tfDoc7.put("laoreet", 1);
      tfDoc7.put("lacus", 1);
      tfDoc7.put("lacinia", 1);
      tfDoc7.put("interdum", 1);
      tfDoc7.put("id", 1);
      tfDoc7.put("hendrerit", 1);
      tfDoc7.put("feugiat", 1);
      tfDoc7.put("eros", 1);
      tfDoc7.put("erat", 1);
      tfDoc7.put("eget", 1);
      tfDoc7.put("egestas", 1);
      tfDoc7.put("dui", 1);
      tfDoc7.put("dolor", 1);
      tfDoc7.put("diam", 1);
      tfDoc7.put("cursus", 1);
      tfDoc7.put("convallis", 1);
      tfDoc7.put("commodo", 1);
      tfDoc7.put("auctor", 1);
      tfDoc7.put("at", 1);
      tfDoc7.put("aliquet", 1);
      tfDoc7.put("adipiscing", 1);
      tfDoc7.put("a", 1);
      TF_DOC_7 = Collections.unmodifiableMap(tfDoc7);
    }

    /**
     * Term frequency values for document 8.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_8;

    static {
      final Map<String, Integer> tfDoc8 = new HashMap<>(77);
      tfDoc8.put("vestibulum", 5);
      tfDoc8.put("nunc", 3);
      tfDoc8.put("mi", 3);
      tfDoc8.put("in", 3);
      tfDoc8.put("egestas", 3);
      tfDoc8.put("augue", 3);
      tfDoc8.put("vitae", 2);
      tfDoc8.put("ut", 2);
      tfDoc8.put("tincidunt", 2);
      tfDoc8.put("sit", 2);
      tfDoc8.put("quisque", 2);
      tfDoc8.put("purus", 2);
      tfDoc8.put("pulvinar", 2);
      tfDoc8.put("praesent", 2);
      tfDoc8.put("orci", 2);
      tfDoc8.put("non", 2);
      tfDoc8.put("neque", 2);
      tfDoc8.put("nec", 2);
      tfDoc8.put("mauris", 2);
      tfDoc8.put("malesuada", 2);
      tfDoc8.put("ligula", 2);
      tfDoc8.put("leo", 2);
      tfDoc8.put("lacus", 2);
      tfDoc8.put("ipsum", 2);
      tfDoc8.put("eu", 2);
      tfDoc8.put("etiam", 2);
      tfDoc8.put("curabitur", 2);
      tfDoc8.put("amet", 2);
      tfDoc8.put("aliquam", 2);
      tfDoc8.put("ac", 2);
      tfDoc8.put("vel", 1);
      tfDoc8.put("urna", 1);
      tfDoc8.put("sollicitudin", 1);
      tfDoc8.put("sodales", 1);
      tfDoc8.put("sed", 1);
      tfDoc8.put("rutrum", 1);
      tfDoc8.put("risus", 1);
      tfDoc8.put("quam", 1);
      tfDoc8.put("platea", 1);
      tfDoc8.put("placerat", 1);
      tfDoc8.put("pellentesque", 1);
      tfDoc8.put("odio", 1);
      tfDoc8.put("nulla", 1);
      tfDoc8.put("nonummy", 1);
      tfDoc8.put("nisl", 1);
      tfDoc8.put("nisi", 1);
      tfDoc8.put("nam", 1);
      tfDoc8.put("molestie", 1);
      tfDoc8.put("metus", 1);
      tfDoc8.put("mattis", 1);
      tfDoc8.put("lorem", 1);
      tfDoc8.put("libero", 1);
      tfDoc8.put("justo", 1);
      tfDoc8.put("interdum", 1);
      tfDoc8.put("id", 1);
      tfDoc8.put("iaculis", 1);
      tfDoc8.put("hac", 1);
      tfDoc8.put("habitasse", 1);
      tfDoc8.put("fusce", 1);
      tfDoc8.put("feugiat", 1);
      tfDoc8.put("fermentum", 1);
      tfDoc8.put("facilisis", 1);
      tfDoc8.put("euismod", 1);
      tfDoc8.put("et", 1);
      tfDoc8.put("est", 1);
      tfDoc8.put("erat", 1);
      tfDoc8.put("enim", 1);
      tfDoc8.put("eleifend", 1);
      tfDoc8.put("dui", 1);
      tfDoc8.put("dolor", 1);
      tfDoc8.put("dictumst", 1);
      tfDoc8.put("dapibus", 1);
      tfDoc8.put("convallis", 1);
      tfDoc8.put("congue", 1);
      tfDoc8.put("auctor", 1);
      tfDoc8.put("adipiscing", 1);
      tfDoc8.put("a", 1);
      TF_DOC_8 = Collections.unmodifiableMap(tfDoc8);
    }

    /**
     * Term frequency values for document 9.
     */
    @SuppressWarnings("PublicStaticCollectionField")
    public static final Map<String, Integer> TF_DOC_9;

    static {
      final Map<String, Integer> tfDoc9 = new HashMap<>(86);
      tfDoc9.put("ut", 6);
      tfDoc9.put("nec", 4);
      tfDoc9.put("turpis", 3);
      tfDoc9.put("quis", 3);
      tfDoc9.put("praesent", 3);
      tfDoc9.put("mi", 3);
      tfDoc9.put("lacus", 3);
      tfDoc9.put("id", 3);
      tfDoc9.put("iaculis", 3);
      tfDoc9.put("faucibus", 3);
      tfDoc9.put("blandit", 3);
      tfDoc9.put("ac", 3);
      tfDoc9.put("a", 3);
      tfDoc9.put("volutpat", 2);
      tfDoc9.put("velit", 2);
      tfDoc9.put("vel", 2);
      tfDoc9.put("tellus", 2);
      tfDoc9.put("suspendisse", 2);
      tfDoc9.put("sed", 2);
      tfDoc9.put("quam", 2);
      tfDoc9.put("purus", 2);
      tfDoc9.put("nulla", 2);
      tfDoc9.put("nisl", 2);
      tfDoc9.put("nisi", 2);
      tfDoc9.put("neque", 2);
      tfDoc9.put("mollis", 2);
      tfDoc9.put("molestie", 2);
      tfDoc9.put("metus", 2);
      tfDoc9.put("mauris", 2);
      tfDoc9.put("ligula", 2);
      tfDoc9.put("in", 2);
      tfDoc9.put("fusce", 2);
      tfDoc9.put("elementum", 2);
      tfDoc9.put("eget", 2);
      tfDoc9.put("egestas", 2);
      tfDoc9.put("dui", 2);
      tfDoc9.put("at", 2);
      tfDoc9.put("ante", 2);
      tfDoc9.put("viverra", 1);
      tfDoc9.put("vestibulum", 1);
      tfDoc9.put("vehicula", 1);
      tfDoc9.put("varius", 1);
      tfDoc9.put("urna", 1);
      tfDoc9.put("ullamcorper", 1);
      tfDoc9.put("sit", 1);
      tfDoc9.put("semper", 1);
      tfDoc9.put("scelerisque", 1);
      tfDoc9.put("sapien", 1);
      tfDoc9.put("sagittis", 1);
      tfDoc9.put("rutrum", 1);
      tfDoc9.put("risus", 1);
      tfDoc9.put("quisque", 1);
      tfDoc9.put("proin", 1);
      tfDoc9.put("porta", 1);
      tfDoc9.put("placerat", 1);
      tfDoc9.put("phasellus", 1);
      tfDoc9.put("pharetra", 1);
      tfDoc9.put("odio", 1);
      tfDoc9.put("nunc", 1);
      tfDoc9.put("non", 1);
      tfDoc9.put("nibh", 1);
      tfDoc9.put("morbi", 1);
      tfDoc9.put("massa", 1);
      tfDoc9.put("libero", 1);
      tfDoc9.put("leo", 1);
      tfDoc9.put("laoreet", 1);
      tfDoc9.put("ipsum", 1);
      tfDoc9.put("gravida", 1);
      tfDoc9.put("fringilla", 1);
      tfDoc9.put("feugiat", 1);
      tfDoc9.put("eu", 1);
      tfDoc9.put("et", 1);
      tfDoc9.put("est", 1);
      tfDoc9.put("eros", 1);
      tfDoc9.put("enim", 1);
      tfDoc9.put("donec", 1);
      tfDoc9.put("dolor", 1);
      tfDoc9.put("dictum", 1);
      tfDoc9.put("cras", 1);
      tfDoc9.put("convallis", 1);
      tfDoc9.put("consectetuer", 1);
      tfDoc9.put("condimentum", 1);
      tfDoc9.put("arcu", 1);
      tfDoc9.put("amet", 1);
      tfDoc9.put("adipiscing", 1);
      tfDoc9.put("accumsan", 1);
      TF_DOC_9 = Collections.unmodifiableMap(tfDoc9);
    }

    /**
     * Get the term-frequency map for a specific document.
     *
     * @param docId Document number
     * @return Term-frequency map for the spcified document
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public static Map<String, Integer> getDocumentTfMap(final int docId) {
      switch (docId) {
        case 0:
          return TF_DOC_0;
        case 1:
          return TF_DOC_1;
        case 2:
          return TF_DOC_2;
        case 3:
          return TF_DOC_3;
        case 4:
          return TF_DOC_4;
        case 5:
          return TF_DOC_5;
        case 6:
          return TF_DOC_6;
        case 7:
          return TF_DOC_7;
        case 8:
          return TF_DOC_8;
        case 9:
          return TF_DOC_9;
        default:
          throw new IllegalArgumentException(
              "Unknown document id. id=" + docId);
      }
    }


  }

  @Override
  public long getTermFrequency() {
    return (long) KnownData.TERM_COUNT;
  }

  @Override
  public void warmUp() {
    // NOP
  }

  @Override
  public Long getTermFrequency(final ByteArray term) {
    assert term != null;
    final Integer tf = KnownData.IDX_TERMFREQ.get(ByteArrayUtils.utf8ToString
        (term));
    if (tf == null) {
      return 0L;
    }
    return tf.longValue();
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    assert term != null;
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
    assert term != null;
    final Long termFrequency = getTermFrequency(term);
    if (termFrequency == null) {
      return 0d;
    }
    return termFrequency.doubleValue() / Long.valueOf(getTermFrequency()).
        doubleValue();
  }

  @Override
  public void close() {
    // NOP
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    return getDocumentsTermsSet(getDocIds());
  }

  @Override
  public long getUniqueTermsCount() {
    return (long) KnownData.TERM_COUNT_UNIQUE;
  }

  @SuppressWarnings("ObjectAllocationInLoop")
  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocumentId(docId);
    final DocumentModel.Builder dmBuilder
        = new DocumentModel.Builder(docId);

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<ByteArray, Long> tfMap = new HashMap<>();

    final Map<String, Integer> kTfMap = KnownData.getDocumentTfMap(docId);

    for (final Map.Entry<String, Integer> entry : kTfMap.entrySet()) {
      try {
        tfMap.put(new ByteArray(entry.getKey().getBytes("UTF-8")),
            entry.getValue().longValue());
      } catch (final UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
    }
    dmBuilder.setTermFrequency(tfMap);
    return dmBuilder.getModel();
  }

  @Override
  public boolean hasDocument(final int docId) {
    return !(docId < 0 || docId > (DOC_COUNT - 1));
  }

  @SuppressWarnings("ObjectAllocationInLoop")
  @Override
  public Iterator<ByteArray> getDocumentsTermsSet(
      final Collection<Integer> docIds) {
    return getDocTermsSet(docIds).iterator();
  }

  @Override
  public Iterator<Map.Entry<ByteArray, Long>> getDocumentsTerms(
      final Collection<Integer> docIds) {
    final Map<ByteArray, Long> tfMap = new HashMap<>();
    for (int docId : docIds) {
      for (Map.Entry<String, Integer> entry : KnownData.getDocumentTfMap
          (docId).entrySet()) {
        try {
          final ByteArray termBa = new ByteArray(entry.getKey().getBytes
              ("UTF-8"));
          if (tfMap.containsKey(termBa)) {
            tfMap.put(termBa, tfMap.get(termBa) + 1L);
          } else {
            tfMap.put(termBa, 1L);
          }
        } catch (UnsupportedEncodingException e) {
          throw new IllegalStateException("Term encoding error!", e);
        }
      }
    }
    return tfMap.entrySet().iterator();
  }


  @Override
  public long getDocumentCount() {
    return (long) DOC_COUNT;
  }

  @Override
  public boolean documentContains(final int documentId, final ByteArray term) {
    assert term != null;
    checkDocumentId(documentId);
    final String termStr = ByteArrayUtils.utf8ToString(term);
    return KnownData.getDocumentTfMap(documentId).containsKey(termStr);
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
    return Collections.emptySet();
  }

  @Override
  public Set<ByteArray> getStopwordsBytes() {
    return Collections.emptySet();
  }

  @Override
  public boolean isClosed() {
    return false;
  }
}
