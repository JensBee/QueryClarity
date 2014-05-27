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
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Source;

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
 * known index to verify calculation results.
 * <p/>
 * This {@link IndexDataProvider} implementation does not support any switching
 * of document fields nor does it respect any stopwords (they will be ignored).
 * <p/>
 * See {@code fixedTestIndexContent.txt} and {@code processFixedTestIndexContent
 * .sh} in {@code src/test/resources/} for reference calculation sources.
 *
 * @author Jens Bertram
 */
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
   * Document contents.
   */
  public static final List<String[]> DOCUMENTS;

  /**
   * Number of fields per document.
   */
  private static final int FIELD_COUNT = 3;
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
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }

    DATA_DIR = new File(TMP_IDX.getIndexDir(), "data");
    if (!DATA_DIR.exists() && !DATA_DIR.mkdirs()) {
      throw new ExceptionInInitializerError(
          "Failed to create data directory: '" + DATA_DIR + "'.");
    }

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
        TMP_IDX.flush();
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }

  /**
   * Number of documents in the index.
   */
  private static final int DOC_COUNT = 10;
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
   * Get a set of unique terms in index.
   *
   * @return Unique set of all index terms
   */
  public Set<ByteArray> getTermSet() {
    return getDocumentsTermSet(getDocumentIds());
  }

  /**
   * Get a unique set of all document ids.
   *
   * @return Document ids
   */
  public Set<Integer> getDocumentIds() {
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
   * Get some unique random terms from the index. The amount of terms returned
   * is the half of all index terms at maximum.
   *
   * @return Tuple containing a set of terms as String and {@link ByteArray}
   * @throws UnsupportedEncodingException Thrown, if a query term could not be
   * encoded to {@code UTF-8}
   */
  public Tuple.Tuple2<Set<String>, Set<ByteArray>>
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
  public Tuple.Tuple2<List<String>, List<ByteArray>> getRandomIndexTerms()
      throws UnsupportedEncodingException {
    final int maxTerm = FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ
        .size() - 1;
    final int qTermCount = RandomValue.getInteger(0, maxTerm / 2);
    final List<ByteArray> qTerms = new ArrayList<>(qTermCount);
    final List<String> qTermsStr = new ArrayList<>(qTermCount);
    final List<String> idxTerms = new ArrayList<>(FixedTestIndexDataProvider
        .KnownData.IDX_TERMFREQ.keySet());

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
   * Data dump of known (pre-calculated) values.
   */
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
     * Number of documents in index.
     */
    public static final int DOC_COUNT = FixedTestIndexDataProvider.DOC_COUNT;
    /**
     * Number of unique combinations of documents & terms. Used for sizing
     * storage maps.
     */
    public static final int DOC_TERM_PAIRS = 711;
    /**
     * Frequency values of all terms in index.
     */
    public static final Map<String, Integer> IDX_TERMFREQ;
    /**
     * Number of fields per document.
     */
    public static final int FIELD_COUNT = FixedTestIndexDataProvider
        .FIELD_COUNT;

    static {
      IDX_TERMFREQ = new HashMap<>(171);
      IDX_TERMFREQ.put("et", 21);
      IDX_TERMFREQ.put("ut", 20);
      IDX_TERMFREQ.put("in", 20);
      IDX_TERMFREQ.put("nec", 18);
      IDX_TERMFREQ.put("vestibulum", 17);
      IDX_TERMFREQ.put("quis", 17);
      IDX_TERMFREQ.put("nunc", 17);
      IDX_TERMFREQ.put("sed", 15);
      IDX_TERMFREQ.put("ac", 15);
      IDX_TERMFREQ.put("eget", 13);
      IDX_TERMFREQ.put("a", 13);
      IDX_TERMFREQ.put("turpis", 12);
      IDX_TERMFREQ.put("phasellus", 12);
      IDX_TERMFREQ.put("mi", 12);
      IDX_TERMFREQ.put("libero", 12);
      IDX_TERMFREQ.put("pellentesque", 11);
      IDX_TERMFREQ.put("metus", 11);
      IDX_TERMFREQ.put("leo", 11);
      IDX_TERMFREQ.put("ipsum", 11);
      IDX_TERMFREQ.put("tincidunt", 10);
      IDX_TERMFREQ.put("posuere", 10);
      IDX_TERMFREQ.put("nisi", 10);
      IDX_TERMFREQ.put("id", 10);
      IDX_TERMFREQ.put("felis", 10);
      IDX_TERMFREQ.put("eu", 10);
      IDX_TERMFREQ.put("donec", 10);
      IDX_TERMFREQ.put("sit", 9);
      IDX_TERMFREQ.put("praesent", 9);
      IDX_TERMFREQ.put("orci", 9);
      IDX_TERMFREQ.put("ligula", 9);
      IDX_TERMFREQ.put("egestas", 9);
      IDX_TERMFREQ.put("dui", 9);
      IDX_TERMFREQ.put("dolor", 9);
      IDX_TERMFREQ.put("augue", 9);
      IDX_TERMFREQ.put("ante", 9);
      IDX_TERMFREQ.put("amet", 9);
      IDX_TERMFREQ.put("aenean", 9);
      IDX_TERMFREQ.put("velit", 8);
      IDX_TERMFREQ.put("vel", 8);
      IDX_TERMFREQ.put("quam", 8);
      IDX_TERMFREQ.put("pede", 8);
      IDX_TERMFREQ.put("non", 8);
      IDX_TERMFREQ.put("neque", 8);
      IDX_TERMFREQ.put("mauris", 8);
      IDX_TERMFREQ.put("lacus", 8);
      IDX_TERMFREQ.put("imperdiet", 8);
      IDX_TERMFREQ.put("fusce", 8);
      IDX_TERMFREQ.put("faucibus", 8);
      IDX_TERMFREQ.put("vitae", 7);
      IDX_TERMFREQ.put("purus", 7);
      IDX_TERMFREQ.put("pretium", 7);
      IDX_TERMFREQ.put("nullam", 7);
      IDX_TERMFREQ.put("nulla", 7);
      IDX_TERMFREQ.put("mollis", 7);
      IDX_TERMFREQ.put("malesuada", 7);
      IDX_TERMFREQ.put("lorem", 7);
      IDX_TERMFREQ.put("feugiat", 7);
      IDX_TERMFREQ.put("blandit", 7);
      IDX_TERMFREQ.put("at", 7);
      IDX_TERMFREQ.put("viverra", 6);
      IDX_TERMFREQ.put("ullamcorper", 6);
      IDX_TERMFREQ.put("tellus", 6);
      IDX_TERMFREQ.put("sem", 6);
      IDX_TERMFREQ.put("sapien", 6);
      IDX_TERMFREQ.put("sagittis", 6);
      IDX_TERMFREQ.put("morbi", 6);
      IDX_TERMFREQ.put("magna", 6);
      IDX_TERMFREQ.put("justo", 6);
      IDX_TERMFREQ.put("hendrerit", 6);
      IDX_TERMFREQ.put("fringilla", 6);
      IDX_TERMFREQ.put("etiam", 6);
      IDX_TERMFREQ.put("eros", 6);
      IDX_TERMFREQ.put("enim", 6);
      IDX_TERMFREQ.put("curabitur", 6);
      IDX_TERMFREQ.put("auctor", 6);
      IDX_TERMFREQ.put("arcu", 6);
      IDX_TERMFREQ.put("adipiscing", 6);
      IDX_TERMFREQ.put("vulputate", 5);
      IDX_TERMFREQ.put("volutpat", 5);
      IDX_TERMFREQ.put("varius", 5);
      IDX_TERMFREQ.put("ultricies", 5);
      IDX_TERMFREQ.put("tortor", 5);
      IDX_TERMFREQ.put("semper", 5);
      IDX_TERMFREQ.put("quisque", 5);
      IDX_TERMFREQ.put("nisl", 5);
      IDX_TERMFREQ.put("maecenas", 5);
      IDX_TERMFREQ.put("luctus", 5);
      IDX_TERMFREQ.put("iaculis", 5);
      IDX_TERMFREQ.put("eleifend", 5);
      IDX_TERMFREQ.put("cursus", 5);
      IDX_TERMFREQ.put("consectetuer", 5);
      IDX_TERMFREQ.put("condimentum", 5);
      IDX_TERMFREQ.put("aliquam", 5);
      IDX_TERMFREQ.put("venenatis", 4);
      IDX_TERMFREQ.put("urna", 4);
      IDX_TERMFREQ.put("ultrices", 4);
      IDX_TERMFREQ.put("sodales", 4);
      IDX_TERMFREQ.put("rutrum", 4);
      IDX_TERMFREQ.put("risus", 4);
      IDX_TERMFREQ.put("rhoncus", 4);
      IDX_TERMFREQ.put("pulvinar", 4);
      IDX_TERMFREQ.put("placerat", 4);
      IDX_TERMFREQ.put("odio", 4);
      IDX_TERMFREQ.put("nonummy", 4);
      IDX_TERMFREQ.put("nibh", 4);
      IDX_TERMFREQ.put("nam", 4);
      IDX_TERMFREQ.put("massa", 4);
      IDX_TERMFREQ.put("laoreet", 4);
      IDX_TERMFREQ.put("euismod", 4);
      IDX_TERMFREQ.put("erat", 4);
      IDX_TERMFREQ.put("elit", 4);
      IDX_TERMFREQ.put("cras", 4);
      IDX_TERMFREQ.put("accumsan", 4);
      IDX_TERMFREQ.put("tristique", 3);
      IDX_TERMFREQ.put("tempus", 3);
      IDX_TERMFREQ.put("tempor", 3);
      IDX_TERMFREQ.put("suspendisse", 3);
      IDX_TERMFREQ.put("primis", 3);
      IDX_TERMFREQ.put("molestie", 3);
      IDX_TERMFREQ.put("mattis", 3);
      IDX_TERMFREQ.put("gravida", 3);
      IDX_TERMFREQ.put("fermentum", 3);
      IDX_TERMFREQ.put("est", 3);
      IDX_TERMFREQ.put("elementum", 3);
      IDX_TERMFREQ.put("dapibus", 3);
      IDX_TERMFREQ.put("curae", 3);
      IDX_TERMFREQ.put("cubilia", 3);
      IDX_TERMFREQ.put("convallis", 3);
      IDX_TERMFREQ.put("consequat", 3);
      IDX_TERMFREQ.put("congue", 3);
      IDX_TERMFREQ.put("vivamus", 2);
      IDX_TERMFREQ.put("vehicula", 2);
      IDX_TERMFREQ.put("suscipit", 2);
      IDX_TERMFREQ.put("sollicitudin", 2);
      IDX_TERMFREQ.put("senectus", 2);
      IDX_TERMFREQ.put("scelerisque", 2);
      IDX_TERMFREQ.put("proin", 2);
      IDX_TERMFREQ.put("porttitor", 2);
      IDX_TERMFREQ.put("porta", 2);
      IDX_TERMFREQ.put("platea", 2);
      IDX_TERMFREQ.put("netus", 2);
      IDX_TERMFREQ.put("lobortis", 2);
      IDX_TERMFREQ.put("lectus", 2);
      IDX_TERMFREQ.put("lacinia", 2);
      IDX_TERMFREQ.put("interdum", 2);
      IDX_TERMFREQ.put("integer", 2);
      IDX_TERMFREQ.put("hac", 2);
      IDX_TERMFREQ.put("habitasse", 2);
      IDX_TERMFREQ.put("habitant", 2);
      IDX_TERMFREQ.put("fames", 2);
      IDX_TERMFREQ.put("facilisis", 2);
      IDX_TERMFREQ.put("duis", 2);
      IDX_TERMFREQ.put("dictumst", 2);
      IDX_TERMFREQ.put("dictum", 2);
      IDX_TERMFREQ.put("diam", 2);
      IDX_TERMFREQ.put("commodo", 2);
      IDX_TERMFREQ.put("bibendum", 2);
      IDX_TERMFREQ.put("aliquet", 2);
      IDX_TERMFREQ.put("sociis", 1);
      IDX_TERMFREQ.put("ridiculus", 1);
      IDX_TERMFREQ.put("pharetra", 1);
      IDX_TERMFREQ.put("penatibus", 1);
      IDX_TERMFREQ.put("parturient", 1);
      IDX_TERMFREQ.put("ornare", 1);
      IDX_TERMFREQ.put("natoque", 1);
      IDX_TERMFREQ.put("nascetur", 1);
      IDX_TERMFREQ.put("mus", 1);
      IDX_TERMFREQ.put("montes", 1);
      IDX_TERMFREQ.put("magnis", 1);
      IDX_TERMFREQ.put("dis", 1);
      IDX_TERMFREQ.put("cum", 1);
    }
    /**
     * Document frequency values of all terms in index.
     */
    public static final Map<String, Integer> IDX_DOCFREQ;
    static {
      IDX_DOCFREQ = new HashMap(171);
      IDX_DOCFREQ.put("a", 9);
      IDX_DOCFREQ.put("ac", 7);
      IDX_DOCFREQ.put("accumsan", 4);
      IDX_DOCFREQ.put("adipiscing", 6);
      IDX_DOCFREQ.put("aenean", 6);
      IDX_DOCFREQ.put("aliquam", 4);
      IDX_DOCFREQ.put("aliquet", 2);
      IDX_DOCFREQ.put("amet", 7);
      IDX_DOCFREQ.put("ante", 6);
      IDX_DOCFREQ.put("arcu", 5);
      IDX_DOCFREQ.put("at", 4);
      IDX_DOCFREQ.put("auctor", 5);
      IDX_DOCFREQ.put("augue", 6);
      IDX_DOCFREQ.put("bibendum", 2);
      IDX_DOCFREQ.put("blandit", 4);
      IDX_DOCFREQ.put("commodo", 2);
      IDX_DOCFREQ.put("condimentum", 4);
      IDX_DOCFREQ.put("congue", 2);
      IDX_DOCFREQ.put("consectetuer", 5);
      IDX_DOCFREQ.put("consequat", 3);
      IDX_DOCFREQ.put("convallis", 3);
      IDX_DOCFREQ.put("cras", 4);
      IDX_DOCFREQ.put("cubilia", 3);
      IDX_DOCFREQ.put("cum", 1);
      IDX_DOCFREQ.put("curabitur", 4);
      IDX_DOCFREQ.put("curae", 3);
      IDX_DOCFREQ.put("cursus", 4);
      IDX_DOCFREQ.put("dapibus", 3);
      IDX_DOCFREQ.put("diam", 2);
      IDX_DOCFREQ.put("dictum", 2);
      IDX_DOCFREQ.put("dictumst", 2);
      IDX_DOCFREQ.put("dis", 1);
      IDX_DOCFREQ.put("dolor", 7);
      IDX_DOCFREQ.put("donec", 8);
      IDX_DOCFREQ.put("dui", 7);
      IDX_DOCFREQ.put("duis", 1);
      IDX_DOCFREQ.put("egestas", 5);
      IDX_DOCFREQ.put("eget", 8);
      IDX_DOCFREQ.put("eleifend", 5);
      IDX_DOCFREQ.put("elementum", 2);
      IDX_DOCFREQ.put("elit", 3);
      IDX_DOCFREQ.put("enim", 5);
      IDX_DOCFREQ.put("erat", 3);
      IDX_DOCFREQ.put("eros", 5);
      IDX_DOCFREQ.put("est", 3);
      IDX_DOCFREQ.put("et", 10);
      IDX_DOCFREQ.put("etiam", 3);
      IDX_DOCFREQ.put("eu", 7);
      IDX_DOCFREQ.put("euismod", 4);
      IDX_DOCFREQ.put("facilisis", 2);
      IDX_DOCFREQ.put("fames", 2);
      IDX_DOCFREQ.put("faucibus", 5);
      IDX_DOCFREQ.put("felis", 3);
      IDX_DOCFREQ.put("fermentum", 2);
      IDX_DOCFREQ.put("feugiat", 7);
      IDX_DOCFREQ.put("fringilla", 5);
      IDX_DOCFREQ.put("fusce", 5);
      IDX_DOCFREQ.put("gravida", 3);
      IDX_DOCFREQ.put("habitant", 2);
      IDX_DOCFREQ.put("habitasse", 2);
      IDX_DOCFREQ.put("hac", 2);
      IDX_DOCFREQ.put("hendrerit", 5);
      IDX_DOCFREQ.put("iaculis", 3);
      IDX_DOCFREQ.put("id", 7);
      IDX_DOCFREQ.put("imperdiet", 4);
      IDX_DOCFREQ.put("in", 10);
      IDX_DOCFREQ.put("integer", 2);
      IDX_DOCFREQ.put("interdum", 2);
      IDX_DOCFREQ.put("ipsum", 9);
      IDX_DOCFREQ.put("justo", 4);
      IDX_DOCFREQ.put("lacinia", 2);
      IDX_DOCFREQ.put("lacus", 5);
      IDX_DOCFREQ.put("laoreet", 4);
      IDX_DOCFREQ.put("lectus", 2);
      IDX_DOCFREQ.put("leo", 7);
      IDX_DOCFREQ.put("libero", 8);
      IDX_DOCFREQ.put("ligula", 6);
      IDX_DOCFREQ.put("lobortis", 2);
      IDX_DOCFREQ.put("lorem", 5);
      IDX_DOCFREQ.put("luctus", 5);
      IDX_DOCFREQ.put("maecenas", 4);
      IDX_DOCFREQ.put("magna", 4);
      IDX_DOCFREQ.put("magnis", 1);
      IDX_DOCFREQ.put("malesuada", 4);
      IDX_DOCFREQ.put("massa", 3);
      IDX_DOCFREQ.put("mattis", 3);
      IDX_DOCFREQ.put("mauris", 5);
      IDX_DOCFREQ.put("metus", 7);
      IDX_DOCFREQ.put("mi", 5);
      IDX_DOCFREQ.put("molestie", 2);
      IDX_DOCFREQ.put("mollis", 6);
      IDX_DOCFREQ.put("montes", 1);
      IDX_DOCFREQ.put("morbi", 4);
      IDX_DOCFREQ.put("mus", 1);
      IDX_DOCFREQ.put("nam", 3);
      IDX_DOCFREQ.put("nascetur", 1);
      IDX_DOCFREQ.put("natoque", 1);
      IDX_DOCFREQ.put("nec", 9);
      IDX_DOCFREQ.put("neque", 6);
      IDX_DOCFREQ.put("netus", 2);
      IDX_DOCFREQ.put("nibh", 4);
      IDX_DOCFREQ.put("nisi", 8);
      IDX_DOCFREQ.put("nisl", 4);
      IDX_DOCFREQ.put("non", 6);
      IDX_DOCFREQ.put("nonummy", 3);
      IDX_DOCFREQ.put("nulla", 6);
      IDX_DOCFREQ.put("nullam", 7);
      IDX_DOCFREQ.put("nunc", 9);
      IDX_DOCFREQ.put("odio", 4);
      IDX_DOCFREQ.put("orci", 6);
      IDX_DOCFREQ.put("ornare", 1);
      IDX_DOCFREQ.put("parturient", 1);
      IDX_DOCFREQ.put("pede", 6);
      IDX_DOCFREQ.put("pellentesque", 6);
      IDX_DOCFREQ.put("penatibus", 1);
      IDX_DOCFREQ.put("pharetra", 1);
      IDX_DOCFREQ.put("phasellus", 7);
      IDX_DOCFREQ.put("placerat", 4);
      IDX_DOCFREQ.put("platea", 2);
      IDX_DOCFREQ.put("porta", 2);
      IDX_DOCFREQ.put("porttitor", 2);
      IDX_DOCFREQ.put("posuere", 4);
      IDX_DOCFREQ.put("praesent", 5);
      IDX_DOCFREQ.put("pretium", 5);
      IDX_DOCFREQ.put("primis", 3);
      IDX_DOCFREQ.put("proin", 2);
      IDX_DOCFREQ.put("pulvinar", 3);
      IDX_DOCFREQ.put("purus", 5);
      IDX_DOCFREQ.put("quam", 6);
      IDX_DOCFREQ.put("quis", 8);
      IDX_DOCFREQ.put("quisque", 4);
      IDX_DOCFREQ.put("rhoncus", 3);
      IDX_DOCFREQ.put("ridiculus", 1);
      IDX_DOCFREQ.put("risus", 4);
      IDX_DOCFREQ.put("rutrum", 4);
      IDX_DOCFREQ.put("sagittis", 5);
      IDX_DOCFREQ.put("sapien", 6);
      IDX_DOCFREQ.put("scelerisque", 2);
      IDX_DOCFREQ.put("sed", 9);
      IDX_DOCFREQ.put("sem", 5);
      IDX_DOCFREQ.put("semper", 4);
      IDX_DOCFREQ.put("senectus", 2);
      IDX_DOCFREQ.put("sit", 7);
      IDX_DOCFREQ.put("sociis", 1);
      IDX_DOCFREQ.put("sodales", 3);
      IDX_DOCFREQ.put("sollicitudin", 2);
      IDX_DOCFREQ.put("suscipit", 2);
      IDX_DOCFREQ.put("suspendisse", 2);
      IDX_DOCFREQ.put("tellus", 4);
      IDX_DOCFREQ.put("tempor", 2);
      IDX_DOCFREQ.put("tempus", 2);
      IDX_DOCFREQ.put("tincidunt", 7);
      IDX_DOCFREQ.put("tortor", 4);
      IDX_DOCFREQ.put("tristique", 2);
      IDX_DOCFREQ.put("turpis", 6);
      IDX_DOCFREQ.put("ullamcorper", 5);
      IDX_DOCFREQ.put("ultrices", 4);
      IDX_DOCFREQ.put("ultricies", 4);
      IDX_DOCFREQ.put("urna", 4);
      IDX_DOCFREQ.put("ut", 9);
      IDX_DOCFREQ.put("varius", 4);
      IDX_DOCFREQ.put("vehicula", 2);
      IDX_DOCFREQ.put("vel", 6);
      IDX_DOCFREQ.put("velit", 5);
      IDX_DOCFREQ.put("venenatis", 4);
      IDX_DOCFREQ.put("vestibulum", 7);
      IDX_DOCFREQ.put("vitae", 5);
      IDX_DOCFREQ.put("vivamus", 2);
      IDX_DOCFREQ.put("viverra", 4);
      IDX_DOCFREQ.put("volutpat", 3);
      IDX_DOCFREQ.put("vulputate", 4);
    }
    /**
     * Term frequency values for document 0.
     */
    public static final Map<String, Integer> TF_DOC_0;
    static {
      TF_DOC_0 = new HashMap<>(65);
      TF_DOC_0.put("justo", 3);
      TF_DOC_0.put("aenean", 3);
      TF_DOC_0.put("vulputate", 2);
      TF_DOC_0.put("quis", 2);
      TF_DOC_0.put("pretium", 2);
      TF_DOC_0.put("pede", 2);
      TF_DOC_0.put("nec", 2);
      TF_DOC_0.put("massa", 2);
      TF_DOC_0.put("felis", 2);
      TF_DOC_0.put("eu", 2);
      TF_DOC_0.put("enim", 2);
      TF_DOC_0.put("eget", 2);
      TF_DOC_0.put("donec", 2);
      TF_DOC_0.put("dolor", 2);
      TF_DOC_0.put("vivamus", 1);
      TF_DOC_0.put("vitae", 1);
      TF_DOC_0.put("venenatis", 1);
      TF_DOC_0.put("vel", 1);
      TF_DOC_0.put("ut", 1);
      TF_DOC_0.put("ultricies", 1);
      TF_DOC_0.put("tincidunt", 1);
      TF_DOC_0.put("tellus", 1);
      TF_DOC_0.put("sociis", 1);
      TF_DOC_0.put("sit", 1);
      TF_DOC_0.put("semper", 1);
      TF_DOC_0.put("sem", 1);
      TF_DOC_0.put("ridiculus", 1);
      TF_DOC_0.put("rhoncus", 1);
      TF_DOC_0.put("quam", 1);
      TF_DOC_0.put("penatibus", 1);
      TF_DOC_0.put("pellentesque", 1);
      TF_DOC_0.put("parturient", 1);
      TF_DOC_0.put("nullam", 1);
      TF_DOC_0.put("nulla", 1);
      TF_DOC_0.put("nisi", 1);
      TF_DOC_0.put("natoque", 1);
      TF_DOC_0.put("nascetur", 1);
      TF_DOC_0.put("mus", 1);
      TF_DOC_0.put("montes", 1);
      TF_DOC_0.put("mollis", 1);
      TF_DOC_0.put("magnis", 1);
      TF_DOC_0.put("lorem", 1);
      TF_DOC_0.put("ligula", 1);
      TF_DOC_0.put("ipsum", 1);
      TF_DOC_0.put("integer", 1);
      TF_DOC_0.put("in", 1);
      TF_DOC_0.put("imperdiet", 1);
      TF_DOC_0.put("fringilla", 1);
      TF_DOC_0.put("et", 1);
      TF_DOC_0.put("elit", 1);
      TF_DOC_0.put("elementum", 1);
      TF_DOC_0.put("eleifend", 1);
      TF_DOC_0.put("dis", 1);
      TF_DOC_0.put("dictum", 1);
      TF_DOC_0.put("dapibus", 1);
      TF_DOC_0.put("cum", 1);
      TF_DOC_0.put("cras", 1);
      TF_DOC_0.put("consequat", 1);
      TF_DOC_0.put("consectetuer", 1);
      TF_DOC_0.put("commodo", 1);
      TF_DOC_0.put("arcu", 1);
      TF_DOC_0.put("amet", 1);
      TF_DOC_0.put("aliquet", 1);
      TF_DOC_0.put("adipiscing", 1);
      TF_DOC_0.put("a", 1);
    }
    /**
     * Term frequency values for document 1.
     */
    public static final Map<String, Integer> TF_DOC_1;
    static {
      TF_DOC_1 = new HashMap<>(70);
      TF_DOC_1.put("etiam", 3);
      TF_DOC_1.put("eget", 3);
      TF_DOC_1.put("ante", 3);
      TF_DOC_1.put("viverra", 2);
      TF_DOC_1.put("vitae", 2);
      TF_DOC_1.put("vel", 2);
      TF_DOC_1.put("ut", 2);
      TF_DOC_1.put("ultricies", 2);
      TF_DOC_1.put("tincidunt", 2);
      TF_DOC_1.put("tempus", 2);
      TF_DOC_1.put("tellus", 2);
      TF_DOC_1.put("sit", 2);
      TF_DOC_1.put("sem", 2);
      TF_DOC_1.put("rhoncus", 2);
      TF_DOC_1.put("quis", 2);
      TF_DOC_1.put("quam", 2);
      TF_DOC_1.put("nisi", 2);
      TF_DOC_1.put("nam", 2);
      TF_DOC_1.put("maecenas", 2);
      TF_DOC_1.put("lorem", 2);
      TF_DOC_1.put("libero", 2);
      TF_DOC_1.put("faucibus", 2);
      TF_DOC_1.put("amet", 2);
      TF_DOC_1.put("aenean", 2);
      TF_DOC_1.put("venenatis", 1);
      TF_DOC_1.put("varius", 1);
      TF_DOC_1.put("ullamcorper", 1);
      TF_DOC_1.put("semper", 1);
      TF_DOC_1.put("sed", 1);
      TF_DOC_1.put("sapien", 1);
      TF_DOC_1.put("rutrum", 1);
      TF_DOC_1.put("quisque", 1);
      TF_DOC_1.put("pulvinar", 1);
      TF_DOC_1.put("porttitor", 1);
      TF_DOC_1.put("phasellus", 1);
      TF_DOC_1.put("orci", 1);
      TF_DOC_1.put("odio", 1);
      TF_DOC_1.put("nunc", 1);
      TF_DOC_1.put("nullam", 1);
      TF_DOC_1.put("nulla", 1);
      TF_DOC_1.put("neque", 1);
      TF_DOC_1.put("nec", 1);
      TF_DOC_1.put("metus", 1);
      TF_DOC_1.put("luctus", 1);
      TF_DOC_1.put("ligula", 1);
      TF_DOC_1.put("leo", 1);
      TF_DOC_1.put("laoreet", 1);
      TF_DOC_1.put("ipsum", 1);
      TF_DOC_1.put("in", 1);
      TF_DOC_1.put("imperdiet", 1);
      TF_DOC_1.put("id", 1);
      TF_DOC_1.put("hendrerit", 1);
      TF_DOC_1.put("feugiat", 1);
      TF_DOC_1.put("eu", 1);
      TF_DOC_1.put("et", 1);
      TF_DOC_1.put("eros", 1);
      TF_DOC_1.put("enim", 1);
      TF_DOC_1.put("eleifend", 1);
      TF_DOC_1.put("dui", 1);
      TF_DOC_1.put("donec", 1);
      TF_DOC_1.put("dapibus", 1);
      TF_DOC_1.put("curabitur", 1);
      TF_DOC_1.put("consequat", 1);
      TF_DOC_1.put("condimentum", 1);
      TF_DOC_1.put("blandit", 1);
      TF_DOC_1.put("augue", 1);
      TF_DOC_1.put("aliquam", 1);
      TF_DOC_1.put("adipiscing", 1);
      TF_DOC_1.put("ac", 1);
      TF_DOC_1.put("a", 1);
    }
    /**
     * Term frequency values for document 2.
     */
    public static final Map<String, Integer> TF_DOC_2;
    static {
      TF_DOC_2 = new HashMap<>(69);
      TF_DOC_2.put("sed", 3);
      TF_DOC_2.put("mi", 3);
      TF_DOC_2.put("in", 3);
      TF_DOC_2.put("vestibulum", 2);
      TF_DOC_2.put("turpis", 2);
      TF_DOC_2.put("sodales", 2);
      TF_DOC_2.put("quis", 2);
      TF_DOC_2.put("magna", 2);
      TF_DOC_2.put("leo", 2);
      TF_DOC_2.put("ipsum", 2);
      TF_DOC_2.put("imperdiet", 2);
      TF_DOC_2.put("fringilla", 2);
      TF_DOC_2.put("et", 2);
      TF_DOC_2.put("eget", 2);
      TF_DOC_2.put("duis", 2);
      TF_DOC_2.put("dui", 2);
      TF_DOC_2.put("arcu", 2);
      TF_DOC_2.put("vulputate", 1);
      TF_DOC_2.put("velit", 1);
      TF_DOC_2.put("ut", 1);
      TF_DOC_2.put("ultricies", 1);
      TF_DOC_2.put("ultrices", 1);
      TF_DOC_2.put("tortor", 1);
      TF_DOC_2.put("suscipit", 1);
      TF_DOC_2.put("sit", 1);
      TF_DOC_2.put("scelerisque", 1);
      TF_DOC_2.put("sapien", 1);
      TF_DOC_2.put("sagittis", 1);
      TF_DOC_2.put("quam", 1);
      TF_DOC_2.put("purus", 1);
      TF_DOC_2.put("primis", 1);
      TF_DOC_2.put("pretium", 1);
      TF_DOC_2.put("posuere", 1);
      TF_DOC_2.put("orci", 1);
      TF_DOC_2.put("nunc", 1);
      TF_DOC_2.put("nullam", 1);
      TF_DOC_2.put("nonummy", 1);
      TF_DOC_2.put("nibh", 1);
      TF_DOC_2.put("nec", 1);
      TF_DOC_2.put("nam", 1);
      TF_DOC_2.put("mollis", 1);
      TF_DOC_2.put("metus", 1);
      TF_DOC_2.put("mauris", 1);
      TF_DOC_2.put("luctus", 1);
      TF_DOC_2.put("lorem", 1);
      TF_DOC_2.put("libero", 1);
      TF_DOC_2.put("lacinia", 1);
      TF_DOC_2.put("id", 1);
      TF_DOC_2.put("iaculis", 1);
      TF_DOC_2.put("hendrerit", 1);
      TF_DOC_2.put("gravida", 1);
      TF_DOC_2.put("fusce", 1);
      TF_DOC_2.put("faucibus", 1);
      TF_DOC_2.put("eu", 1);
      TF_DOC_2.put("eleifend", 1);
      TF_DOC_2.put("donec", 1);
      TF_DOC_2.put("cursus", 1);
      TF_DOC_2.put("curae", 1);
      TF_DOC_2.put("cubilia", 1);
      TF_DOC_2.put("cras", 1);
      TF_DOC_2.put("consequat", 1);
      TF_DOC_2.put("consectetuer", 1);
      TF_DOC_2.put("bibendum", 1);
      TF_DOC_2.put("augue", 1);
      TF_DOC_2.put("ante", 1);
      TF_DOC_2.put("amet", 1);
      TF_DOC_2.put("accumsan", 1);
      TF_DOC_2.put("ac", 1);
      TF_DOC_2.put("a", 1);
    }
    /**
     * Term frequency values for document 3.
     */
    public static final Map<String, Integer> TF_DOC_3;
    static {
      TF_DOC_3 = new HashMap<>(71);
      TF_DOC_3.put("imperdiet", 4);
      TF_DOC_3.put("phasellus", 3);
      TF_DOC_3.put("nunc", 3);
      TF_DOC_3.put("vestibulum", 2);
      TF_DOC_3.put("ut", 2);
      TF_DOC_3.put("sed", 2);
      TF_DOC_3.put("posuere", 2);
      TF_DOC_3.put("nonummy", 2);
      TF_DOC_3.put("non", 2);
      TF_DOC_3.put("nec", 2);
      TF_DOC_3.put("mauris", 2);
      TF_DOC_3.put("leo", 2);
      TF_DOC_3.put("id", 2);
      TF_DOC_3.put("hendrerit", 2);
      TF_DOC_3.put("et", 2);
      TF_DOC_3.put("eros", 2);
      TF_DOC_3.put("volutpat", 1);
      TF_DOC_3.put("vitae", 1);
      TF_DOC_3.put("ultricies", 1);
      TF_DOC_3.put("ultrices", 1);
      TF_DOC_3.put("ullamcorper", 1);
      TF_DOC_3.put("tincidunt", 1);
      TF_DOC_3.put("tempus", 1);
      TF_DOC_3.put("sit", 1);
      TF_DOC_3.put("sem", 1);
      TF_DOC_3.put("sapien", 1);
      TF_DOC_3.put("sagittis", 1);
      TF_DOC_3.put("rutrum", 1);
      TF_DOC_3.put("risus", 1);
      TF_DOC_3.put("quis", 1);
      TF_DOC_3.put("pretium", 1);
      TF_DOC_3.put("praesent", 1);
      TF_DOC_3.put("pellentesque", 1);
      TF_DOC_3.put("pede", 1);
      TF_DOC_3.put("orci", 1);
      TF_DOC_3.put("nullam", 1);
      TF_DOC_3.put("nulla", 1);
      TF_DOC_3.put("nisl", 1);
      TF_DOC_3.put("nisi", 1);
      TF_DOC_3.put("neque", 1);
      TF_DOC_3.put("mollis", 1);
      TF_DOC_3.put("metus", 1);
      TF_DOC_3.put("malesuada", 1);
      TF_DOC_3.put("maecenas", 1);
      TF_DOC_3.put("ligula", 1);
      TF_DOC_3.put("libero", 1);
      TF_DOC_3.put("lectus", 1);
      TF_DOC_3.put("justo", 1);
      TF_DOC_3.put("ipsum", 1);
      TF_DOC_3.put("integer", 1);
      TF_DOC_3.put("in", 1);
      TF_DOC_3.put("feugiat", 1);
      TF_DOC_3.put("facilisis", 1);
      TF_DOC_3.put("euismod", 1);
      TF_DOC_3.put("etiam", 1);
      TF_DOC_3.put("eget", 1);
      TF_DOC_3.put("dui", 1);
      TF_DOC_3.put("donec", 1);
      TF_DOC_3.put("dolor", 1);
      TF_DOC_3.put("curabitur", 1);
      TF_DOC_3.put("cras", 1);
      TF_DOC_3.put("consectetuer", 1);
      TF_DOC_3.put("auctor", 1);
      TF_DOC_3.put("arcu", 1);
      TF_DOC_3.put("ante", 1);
      TF_DOC_3.put("amet", 1);
      TF_DOC_3.put("aliquam", 1);
      TF_DOC_3.put("aenean", 1);
      TF_DOC_3.put("adipiscing", 1);
      TF_DOC_3.put("accumsan", 1);
      TF_DOC_3.put("a", 1);
    }
    /**
     * Term frequency values for document 4.
     */
    public static final Map<String, Integer> TF_DOC_4;
    static {
      TF_DOC_4 = new HashMap<>(53);
      TF_DOC_4.put("posuere", 4);
      TF_DOC_4.put("vestibulum", 3);
      TF_DOC_4.put("turpis", 3);
      TF_DOC_4.put("sed", 3);
      TF_DOC_4.put("orci", 3);
      TF_DOC_4.put("quis", 2);
      TF_DOC_4.put("praesent", 2);
      TF_DOC_4.put("phasellus", 2);
      TF_DOC_4.put("in", 2);
      TF_DOC_4.put("erat", 2);
      TF_DOC_4.put("elit", 2);
      TF_DOC_4.put("cursus", 2);
      TF_DOC_4.put("congue", 2);
      TF_DOC_4.put("ac", 2);
      TF_DOC_4.put("vulputate", 1);
      TF_DOC_4.put("vitae", 1);
      TF_DOC_4.put("velit", 1);
      TF_DOC_4.put("ultrices", 1);
      TF_DOC_4.put("tortor", 1);
      TF_DOC_4.put("tellus", 1);
      TF_DOC_4.put("sit", 1);
      TF_DOC_4.put("primis", 1);
      TF_DOC_4.put("porttitor", 1);
      TF_DOC_4.put("placerat", 1);
      TF_DOC_4.put("pellentesque", 1);
      TF_DOC_4.put("pede", 1);
      TF_DOC_4.put("nunc", 1);
      TF_DOC_4.put("non", 1);
      TF_DOC_4.put("nisi", 1);
      TF_DOC_4.put("metus", 1);
      TF_DOC_4.put("mattis", 1);
      TF_DOC_4.put("massa", 1);
      TF_DOC_4.put("luctus", 1);
      TF_DOC_4.put("lectus", 1);
      TF_DOC_4.put("ipsum", 1);
      TF_DOC_4.put("fringilla", 1);
      TF_DOC_4.put("faucibus", 1);
      TF_DOC_4.put("euismod", 1);
      TF_DOC_4.put("et", 1);
      TF_DOC_4.put("donec", 1);
      TF_DOC_4.put("dolor", 1);
      TF_DOC_4.put("curae", 1);
      TF_DOC_4.put("cubilia", 1);
      TF_DOC_4.put("consectetuer", 1);
      TF_DOC_4.put("bibendum", 1);
      TF_DOC_4.put("augue", 1);
      TF_DOC_4.put("at", 1);
      TF_DOC_4.put("arcu", 1);
      TF_DOC_4.put("ante", 1);
      TF_DOC_4.put("amet", 1);
      TF_DOC_4.put("aliquam", 1);
      TF_DOC_4.put("aenean", 1);
      TF_DOC_4.put("accumsan", 1);
    }
    /**
     * Term frequency values for document 5.
     */
    public static final Map<String, Integer> TF_DOC_5;
    static {
      TF_DOC_5 = new HashMap<>(74);
      TF_DOC_5.put("et", 4);
      TF_DOC_5.put("vestibulum", 3);
      TF_DOC_5.put("posuere", 3);
      TF_DOC_5.put("pellentesque", 3);
      TF_DOC_5.put("nunc", 3);
      TF_DOC_5.put("libero", 3);
      TF_DOC_5.put("volutpat", 2);
      TF_DOC_5.put("ut", 2);
      TF_DOC_5.put("turpis", 2);
      TF_DOC_5.put("sagittis", 2);
      TF_DOC_5.put("nec", 2);
      TF_DOC_5.put("in", 2);
      TF_DOC_5.put("dolor", 2);
      TF_DOC_5.put("augue", 2);
      TF_DOC_5.put("ac", 2);
      TF_DOC_5.put("venenatis", 1);
      TF_DOC_5.put("velit", 1);
      TF_DOC_5.put("varius", 1);
      TF_DOC_5.put("urna", 1);
      TF_DOC_5.put("ultrices", 1);
      TF_DOC_5.put("tristique", 1);
      TF_DOC_5.put("tortor", 1);
      TF_DOC_5.put("tincidunt", 1);
      TF_DOC_5.put("tempor", 1);
      TF_DOC_5.put("suspendisse", 1);
      TF_DOC_5.put("suscipit", 1);
      TF_DOC_5.put("sollicitudin", 1);
      TF_DOC_5.put("sodales", 1);
      TF_DOC_5.put("senectus", 1);
      TF_DOC_5.put("sem", 1);
      TF_DOC_5.put("sed", 1);
      TF_DOC_5.put("quis", 1);
      TF_DOC_5.put("purus", 1);
      TF_DOC_5.put("pulvinar", 1);
      TF_DOC_5.put("primis", 1);
      TF_DOC_5.put("phasellus", 1);
      TF_DOC_5.put("pede", 1);
      TF_DOC_5.put("orci", 1);
      TF_DOC_5.put("nullam", 1);
      TF_DOC_5.put("non", 1);
      TF_DOC_5.put("nibh", 1);
      TF_DOC_5.put("netus", 1);
      TF_DOC_5.put("neque", 1);
      TF_DOC_5.put("morbi", 1);
      TF_DOC_5.put("mollis", 1);
      TF_DOC_5.put("malesuada", 1);
      TF_DOC_5.put("magna", 1);
      TF_DOC_5.put("maecenas", 1);
      TF_DOC_5.put("luctus", 1);
      TF_DOC_5.put("lacus", 1);
      TF_DOC_5.put("justo", 1);
      TF_DOC_5.put("ipsum", 1);
      TF_DOC_5.put("id", 1);
      TF_DOC_5.put("habitant", 1);
      TF_DOC_5.put("fusce", 1);
      TF_DOC_5.put("feugiat", 1);
      TF_DOC_5.put("faucibus", 1);
      TF_DOC_5.put("fames", 1);
      TF_DOC_5.put("eu", 1);
      TF_DOC_5.put("elit", 1);
      TF_DOC_5.put("eget", 1);
      TF_DOC_5.put("egestas", 1);
      TF_DOC_5.put("dui", 1);
      TF_DOC_5.put("donec", 1);
      TF_DOC_5.put("diam", 1);
      TF_DOC_5.put("cursus", 1);
      TF_DOC_5.put("curae", 1);
      TF_DOC_5.put("cubilia", 1);
      TF_DOC_5.put("condimentum", 1);
      TF_DOC_5.put("blandit", 1);
      TF_DOC_5.put("auctor", 1);
      TF_DOC_5.put("ante", 1);
      TF_DOC_5.put("aenean", 1);
      TF_DOC_5.put("a", 1);
    }
    /**
     * Term frequency values for document 6.
     */
    public static final Map<String, Integer> TF_DOC_6;
    static {
      TF_DOC_6 = new HashMap<>(72);
      TF_DOC_6.put("felis", 5);
      TF_DOC_6.put("quis", 4);
      TF_DOC_6.put("pellentesque", 4);
      TF_DOC_6.put("ac", 4);
      TF_DOC_6.put("velit", 3);
      TF_DOC_6.put("phasellus", 3);
      TF_DOC_6.put("morbi", 3);
      TF_DOC_6.put("malesuada", 3);
      TF_DOC_6.put("in", 3);
      TF_DOC_6.put("et", 3);
      TF_DOC_6.put("at", 3);
      TF_DOC_6.put("a", 3);
      TF_DOC_6.put("ut", 2);
      TF_DOC_6.put("tristique", 2);
      TF_DOC_6.put("tortor", 2);
      TF_DOC_6.put("tincidunt", 2);
      TF_DOC_6.put("semper", 2);
      TF_DOC_6.put("pretium", 2);
      TF_DOC_6.put("pede", 2);
      TF_DOC_6.put("nunc", 2);
      TF_DOC_6.put("nec", 2);
      TF_DOC_6.put("mi", 2);
      TF_DOC_6.put("leo", 2);
      TF_DOC_6.put("egestas", 2);
      TF_DOC_6.put("curabitur", 2);
      TF_DOC_6.put("auctor", 2);
      TF_DOC_6.put("viverra", 1);
      TF_DOC_6.put("vivamus", 1);
      TF_DOC_6.put("vel", 1);
      TF_DOC_6.put("vehicula", 1);
      TF_DOC_6.put("urna", 1);
      TF_DOC_6.put("ullamcorper", 1);
      TF_DOC_6.put("turpis", 1);
      TF_DOC_6.put("senectus", 1);
      TF_DOC_6.put("sem", 1);
      TF_DOC_6.put("sed", 1);
      TF_DOC_6.put("sapien", 1);
      TF_DOC_6.put("rhoncus", 1);
      TF_DOC_6.put("quam", 1);
      TF_DOC_6.put("proin", 1);
      TF_DOC_6.put("porta", 1);
      TF_DOC_6.put("platea", 1);
      TF_DOC_6.put("ornare", 1);
      TF_DOC_6.put("nullam", 1);
      TF_DOC_6.put("non", 1);
      TF_DOC_6.put("nisi", 1);
      TF_DOC_6.put("netus", 1);
      TF_DOC_6.put("neque", 1);
      TF_DOC_6.put("mattis", 1);
      TF_DOC_6.put("magna", 1);
      TF_DOC_6.put("lobortis", 1);
      TF_DOC_6.put("libero", 1);
      TF_DOC_6.put("laoreet", 1);
      TF_DOC_6.put("lacus", 1);
      TF_DOC_6.put("ipsum", 1);
      TF_DOC_6.put("hendrerit", 1);
      TF_DOC_6.put("hac", 1);
      TF_DOC_6.put("habitasse", 1);
      TF_DOC_6.put("habitant", 1);
      TF_DOC_6.put("gravida", 1);
      TF_DOC_6.put("fringilla", 1);
      TF_DOC_6.put("feugiat", 1);
      TF_DOC_6.put("fames", 1);
      TF_DOC_6.put("euismod", 1);
      TF_DOC_6.put("est", 1);
      TF_DOC_6.put("eros", 1);
      TF_DOC_6.put("enim", 1);
      TF_DOC_6.put("eleifend", 1);
      TF_DOC_6.put("eget", 1);
      TF_DOC_6.put("dictumst", 1);
      TF_DOC_6.put("augue", 1);
      TF_DOC_6.put("aenean", 1);
    }
    /**
     * Term frequency values for document 7.
     */
    public static final Map<String, Integer> TF_DOC_7;
    static {
      TF_DOC_7 = new HashMap<>(74);
      TF_DOC_7.put("et", 5);
      TF_DOC_7.put("metus", 4);
      TF_DOC_7.put("fusce", 3);
      TF_DOC_7.put("felis", 3);
      TF_DOC_7.put("viverra", 2);
      TF_DOC_7.put("varius", 2);
      TF_DOC_7.put("ut", 2);
      TF_DOC_7.put("ullamcorper", 2);
      TF_DOC_7.put("tempor", 2);
      TF_DOC_7.put("nunc", 2);
      TF_DOC_7.put("nec", 2);
      TF_DOC_7.put("magna", 2);
      TF_DOC_7.put("lorem", 2);
      TF_DOC_7.put("ligula", 2);
      TF_DOC_7.put("libero", 2);
      TF_DOC_7.put("in", 2);
      TF_DOC_7.put("fermentum", 2);
      TF_DOC_7.put("eu", 2);
      TF_DOC_7.put("donec", 2);
      TF_DOC_7.put("condimentum", 2);
      TF_DOC_7.put("blandit", 2);
      TF_DOC_7.put("vulputate", 1);
      TF_DOC_7.put("vestibulum", 1);
      TF_DOC_7.put("venenatis", 1);
      TF_DOC_7.put("vel", 1);
      TF_DOC_7.put("turpis", 1);
      TF_DOC_7.put("tincidunt", 1);
      TF_DOC_7.put("sed", 1);
      TF_DOC_7.put("sapien", 1);
      TF_DOC_7.put("sagittis", 1);
      TF_DOC_7.put("risus", 1);
      TF_DOC_7.put("quisque", 1);
      TF_DOC_7.put("purus", 1);
      TF_DOC_7.put("pretium", 1);
      TF_DOC_7.put("praesent", 1);
      TF_DOC_7.put("placerat", 1);
      TF_DOC_7.put("phasellus", 1);
      TF_DOC_7.put("pede", 1);
      TF_DOC_7.put("odio", 1);
      TF_DOC_7.put("nullam", 1);
      TF_DOC_7.put("nulla", 1);
      TF_DOC_7.put("nisl", 1);
      TF_DOC_7.put("nisi", 1);
      TF_DOC_7.put("nibh", 1);
      TF_DOC_7.put("morbi", 1);
      TF_DOC_7.put("mollis", 1);
      TF_DOC_7.put("mi", 1);
      TF_DOC_7.put("mauris", 1);
      TF_DOC_7.put("maecenas", 1);
      TF_DOC_7.put("luctus", 1);
      TF_DOC_7.put("lobortis", 1);
      TF_DOC_7.put("leo", 1);
      TF_DOC_7.put("laoreet", 1);
      TF_DOC_7.put("lacus", 1);
      TF_DOC_7.put("lacinia", 1);
      TF_DOC_7.put("interdum", 1);
      TF_DOC_7.put("id", 1);
      TF_DOC_7.put("hendrerit", 1);
      TF_DOC_7.put("feugiat", 1);
      TF_DOC_7.put("eros", 1);
      TF_DOC_7.put("erat", 1);
      TF_DOC_7.put("eget", 1);
      TF_DOC_7.put("egestas", 1);
      TF_DOC_7.put("dui", 1);
      TF_DOC_7.put("dolor", 1);
      TF_DOC_7.put("diam", 1);
      TF_DOC_7.put("cursus", 1);
      TF_DOC_7.put("convallis", 1);
      TF_DOC_7.put("commodo", 1);
      TF_DOC_7.put("auctor", 1);
      TF_DOC_7.put("at", 1);
      TF_DOC_7.put("aliquet", 1);
      TF_DOC_7.put("adipiscing", 1);
      TF_DOC_7.put("a", 1);
    }
    /**
     * Term frequency values for document 8.
     */
    public static final Map<String, Integer> TF_DOC_8;
    static {
      TF_DOC_8 = new HashMap<>(77);
      TF_DOC_8.put("vestibulum", 5);
      TF_DOC_8.put("nunc", 3);
      TF_DOC_8.put("mi", 3);
      TF_DOC_8.put("in", 3);
      TF_DOC_8.put("egestas", 3);
      TF_DOC_8.put("augue", 3);
      TF_DOC_8.put("vitae", 2);
      TF_DOC_8.put("ut", 2);
      TF_DOC_8.put("tincidunt", 2);
      TF_DOC_8.put("sit", 2);
      TF_DOC_8.put("quisque", 2);
      TF_DOC_8.put("purus", 2);
      TF_DOC_8.put("pulvinar", 2);
      TF_DOC_8.put("praesent", 2);
      TF_DOC_8.put("orci", 2);
      TF_DOC_8.put("non", 2);
      TF_DOC_8.put("neque", 2);
      TF_DOC_8.put("nec", 2);
      TF_DOC_8.put("mauris", 2);
      TF_DOC_8.put("malesuada", 2);
      TF_DOC_8.put("ligula", 2);
      TF_DOC_8.put("leo", 2);
      TF_DOC_8.put("lacus", 2);
      TF_DOC_8.put("ipsum", 2);
      TF_DOC_8.put("eu", 2);
      TF_DOC_8.put("etiam", 2);
      TF_DOC_8.put("curabitur", 2);
      TF_DOC_8.put("amet", 2);
      TF_DOC_8.put("aliquam", 2);
      TF_DOC_8.put("ac", 2);
      TF_DOC_8.put("vel", 1);
      TF_DOC_8.put("urna", 1);
      TF_DOC_8.put("sollicitudin", 1);
      TF_DOC_8.put("sodales", 1);
      TF_DOC_8.put("sed", 1);
      TF_DOC_8.put("rutrum", 1);
      TF_DOC_8.put("risus", 1);
      TF_DOC_8.put("quam", 1);
      TF_DOC_8.put("platea", 1);
      TF_DOC_8.put("placerat", 1);
      TF_DOC_8.put("pellentesque", 1);
      TF_DOC_8.put("odio", 1);
      TF_DOC_8.put("nulla", 1);
      TF_DOC_8.put("nonummy", 1);
      TF_DOC_8.put("nisl", 1);
      TF_DOC_8.put("nisi", 1);
      TF_DOC_8.put("nam", 1);
      TF_DOC_8.put("molestie", 1);
      TF_DOC_8.put("metus", 1);
      TF_DOC_8.put("mattis", 1);
      TF_DOC_8.put("lorem", 1);
      TF_DOC_8.put("libero", 1);
      TF_DOC_8.put("justo", 1);
      TF_DOC_8.put("interdum", 1);
      TF_DOC_8.put("id", 1);
      TF_DOC_8.put("iaculis", 1);
      TF_DOC_8.put("hac", 1);
      TF_DOC_8.put("habitasse", 1);
      TF_DOC_8.put("fusce", 1);
      TF_DOC_8.put("feugiat", 1);
      TF_DOC_8.put("fermentum", 1);
      TF_DOC_8.put("facilisis", 1);
      TF_DOC_8.put("euismod", 1);
      TF_DOC_8.put("et", 1);
      TF_DOC_8.put("est", 1);
      TF_DOC_8.put("erat", 1);
      TF_DOC_8.put("enim", 1);
      TF_DOC_8.put("eleifend", 1);
      TF_DOC_8.put("dui", 1);
      TF_DOC_8.put("dolor", 1);
      TF_DOC_8.put("dictumst", 1);
      TF_DOC_8.put("dapibus", 1);
      TF_DOC_8.put("convallis", 1);
      TF_DOC_8.put("congue", 1);
      TF_DOC_8.put("auctor", 1);
      TF_DOC_8.put("adipiscing", 1);
      TF_DOC_8.put("a", 1);
    }
    /**
     * Term frequency values for document 9.
     */
    public static final Map<String, Integer> TF_DOC_9;
    static {
      TF_DOC_9 = new HashMap<>(86);
      TF_DOC_9.put("ut", 6);
      TF_DOC_9.put("nec", 4);
      TF_DOC_9.put("turpis", 3);
      TF_DOC_9.put("quis", 3);
      TF_DOC_9.put("praesent", 3);
      TF_DOC_9.put("mi", 3);
      TF_DOC_9.put("lacus", 3);
      TF_DOC_9.put("id", 3);
      TF_DOC_9.put("iaculis", 3);
      TF_DOC_9.put("faucibus", 3);
      TF_DOC_9.put("blandit", 3);
      TF_DOC_9.put("ac", 3);
      TF_DOC_9.put("a", 3);
      TF_DOC_9.put("volutpat", 2);
      TF_DOC_9.put("velit", 2);
      TF_DOC_9.put("vel", 2);
      TF_DOC_9.put("tellus", 2);
      TF_DOC_9.put("suspendisse", 2);
      TF_DOC_9.put("sed", 2);
      TF_DOC_9.put("quam", 2);
      TF_DOC_9.put("purus", 2);
      TF_DOC_9.put("nulla", 2);
      TF_DOC_9.put("nisl", 2);
      TF_DOC_9.put("nisi", 2);
      TF_DOC_9.put("neque", 2);
      TF_DOC_9.put("mollis", 2);
      TF_DOC_9.put("molestie", 2);
      TF_DOC_9.put("metus", 2);
      TF_DOC_9.put("mauris", 2);
      TF_DOC_9.put("ligula", 2);
      TF_DOC_9.put("in", 2);
      TF_DOC_9.put("fusce", 2);
      TF_DOC_9.put("elementum", 2);
      TF_DOC_9.put("eget", 2);
      TF_DOC_9.put("egestas", 2);
      TF_DOC_9.put("dui", 2);
      TF_DOC_9.put("at", 2);
      TF_DOC_9.put("ante", 2);
      TF_DOC_9.put("viverra", 1);
      TF_DOC_9.put("vestibulum", 1);
      TF_DOC_9.put("vehicula", 1);
      TF_DOC_9.put("varius", 1);
      TF_DOC_9.put("urna", 1);
      TF_DOC_9.put("ullamcorper", 1);
      TF_DOC_9.put("sit", 1);
      TF_DOC_9.put("semper", 1);
      TF_DOC_9.put("scelerisque", 1);
      TF_DOC_9.put("sapien", 1);
      TF_DOC_9.put("sagittis", 1);
      TF_DOC_9.put("rutrum", 1);
      TF_DOC_9.put("risus", 1);
      TF_DOC_9.put("quisque", 1);
      TF_DOC_9.put("proin", 1);
      TF_DOC_9.put("porta", 1);
      TF_DOC_9.put("placerat", 1);
      TF_DOC_9.put("phasellus", 1);
      TF_DOC_9.put("pharetra", 1);
      TF_DOC_9.put("odio", 1);
      TF_DOC_9.put("nunc", 1);
      TF_DOC_9.put("non", 1);
      TF_DOC_9.put("nibh", 1);
      TF_DOC_9.put("morbi", 1);
      TF_DOC_9.put("massa", 1);
      TF_DOC_9.put("libero", 1);
      TF_DOC_9.put("leo", 1);
      TF_DOC_9.put("laoreet", 1);
      TF_DOC_9.put("ipsum", 1);
      TF_DOC_9.put("gravida", 1);
      TF_DOC_9.put("fringilla", 1);
      TF_DOC_9.put("feugiat", 1);
      TF_DOC_9.put("eu", 1);
      TF_DOC_9.put("et", 1);
      TF_DOC_9.put("est", 1);
      TF_DOC_9.put("eros", 1);
      TF_DOC_9.put("enim", 1);
      TF_DOC_9.put("donec", 1);
      TF_DOC_9.put("dolor", 1);
      TF_DOC_9.put("dictum", 1);
      TF_DOC_9.put("cras", 1);
      TF_DOC_9.put("convallis", 1);
      TF_DOC_9.put("consectetuer", 1);
      TF_DOC_9.put("condimentum", 1);
      TF_DOC_9.put("arcu", 1);
      TF_DOC_9.put("amet", 1);
      TF_DOC_9.put("adipiscing", 1);
      TF_DOC_9.put("accumsan", 1);
    }

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
    return KnownData.TERM_COUNT;
  }

  @Override
  public void warmUp()
      throws DataProviderException {
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
    return getDocumentsTermSet(getDocumentIds()).iterator();
  }

  @Override
  public Source<ByteArray> getTermsSource() {
    return new CollectionSource<>(getDocumentsTermSet(getDocumentIds()));
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
    return KnownData.TERM_COUNT_UNIQUE;
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocumentId(docId);
    final DocumentModel.Builder dmBuilder
        = new DocumentModel.Builder(docId);

    final Map<ByteArray, Long> tfMap = new HashMap<>();

    final Map<String, Integer> kTfMap = KnownData.getDocumentTfMap(docId);

    for (Map.Entry<String, Integer> entry : kTfMap.entrySet()) {
      try {
        tfMap.put(new ByteArray(entry.getKey().getBytes("UTF-8")),
            entry.getValue().longValue());
      } catch (UnsupportedEncodingException e) {
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

  @Override
  public Set<ByteArray> getDocumentsTermSet(
      final Collection<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      throw new IllegalArgumentException("Empty document id list.");
    }
    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Set<ByteArray> terms = new HashSet<>();

    for (final Integer documentId : uniqueDocIds) {
      checkDocumentId(documentId);
      for (String term : KnownData.getDocumentTfMap(documentId).keySet()) {
        try {
          terms.add(new ByteArray(term.getBytes("UTF-8")));
        } catch (UnsupportedEncodingException e) {
          throw new IllegalStateException(e);
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
    return Collections.<String>emptySet();
  }

  @Override
  public Set<ByteArray> getStopwordsBytes() {
    return Collections
        .<ByteArray>emptySet();
  }

  @Override
  public boolean isDisposed() {
    return false;
  }
}
