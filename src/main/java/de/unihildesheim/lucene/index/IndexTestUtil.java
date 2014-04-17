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
import de.unihildesheim.Tuple;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for testing index related functions.
 *
 *
 */
public final class IndexTestUtil {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          IndexTestUtil.class);

  /**
   * Pick some (1 to n) terms from the index and sets them as stop-words and
   * set random index fields active for the {@link Environment}.
   *
   * @param dataProv Data provider
   */
  public static void setRandomStopWordsAndFields(
          final TestIndexDataProvider dataProv) {
    throw new UnsupportedOperationException("BROKEN!");
//    final Collection<String> stopWords = setRandomStopWords(dataProv, false);
//    final Collection<String> fields = setRandomFields(dataProv, false);
//    Environment.setFieldsAndWords(fields.toArray(new String[fields.size()]),
//            stopWords);
  }

  /**
   * Picks some (1 to n) terms from the index and sets them as stop-words.
   *
   * @param dataProv Data provider
   * @param set If true, set new stop-words to {@link Environment}
   * @return Stop words term collection
   */
  private static Collection<String> setRandomStopWords(
          final TestIndexDataProvider dataProv, final boolean set) {
    throw new UnsupportedOperationException("BROKEN!");
//    Iterator<ByteArray> termsIt = dataProv.getTermsIterator();
//    @SuppressWarnings(value = "CollectionWithoutInitialCapacity")
//    final Collection<String> stopWords = new ArrayList<>();
//    while (termsIt.hasNext()) {
//      if (RandomValue.getBoolean()) {
//        stopWords.add(ByteArrayUtil.utf8ToString(termsIt.next()));
//      } else {
//        termsIt.next();
//      }
//    }
//    if (stopWords.isEmpty()) {
//      stopWords.add(ByteArrayUtil.utf8ToString(new ArrayList<>(
//              dataProv.getTermSet()).get(0)));
//    }
//    if (set) {
//      Environment.setStopwords(stopWords);
//    }
//    return stopWords;
  }

  /**
   * Picks some (1 to n) terms from the index and sets them as stop-words.
   *
   * @param dataProv Data provider
   * @return Stop words term collection
   */
  public static Collection<String> setRandomStopWords(
          final TestIndexDataProvider dataProv) {
    return setRandomStopWords(dataProv, true);
  }

  /**
   * Set random index fields active for the {@link Environment}.
   *
   * @param index {@link TestIndexDataProvider}
   * @param set If true, set fields to {@link Environment}
   * @return List of fields set
   */
  private static Collection<String> setRandomFields(
          final TestIndexDataProvider index, final boolean set) {
    throw new UnsupportedOperationException("BROKEN!");
//    Collection<String> fields = index.getRandomFields();
//    if (set) {
//      Environment.setFields(fields.toArray(new String[fields.size()]));
//    }
//    return fields;
  }

  /**
   * Set random index fields active for the {@link Environment}.
   *
   * @param index {@link TestIndexDataProvider}
   * @return List of fields set
   */
  public static Collection<String> setRandomFields(
          final TestIndexDataProvider index) {
    return setRandomFields(index, true);
  }

  /**
   * Set all index fields active for the {@link Environment}.
   *
   * @param index {@link TestIndexDataProvider}
   */
  public static void setAllFields(final TestIndexDataProvider index) {
    throw new UnsupportedOperationException("BROKEN!");
//    index.enableAllFields();
//    Collection<String> fields = index.getActiveFieldNames();
//    Environment.setFields(fields.toArray(new String[fields.size()]));
  }

  /**
   * Get the list of stopwords currently set in the {@link Environment}.
   *
   * @return Collection of stopwords or <tt>null</tt>, if none are set
   * @throws UnsupportedEncodingException Thrown, if a stopword could not be
   * proper encoded
   */
  public static Collection<ByteArray> getStopwordsFromEnvironment() throws
          UnsupportedEncodingException {
    final Collection<String> stopwordsStr = Environment.getStopwords();
    if (!stopwordsStr.isEmpty()) {
      LOG.debug("Excluding stopwords: {}", stopwordsStr);
      final Collection<ByteArray> stopwords = new ArrayList<>(stopwordsStr.
              size());
      for (String sw : stopwordsStr) {
        stopwords.add(new ByteArray(sw.getBytes("UTF-8")));
      }
      return stopwords;
    }
    return null;
  }

  /**
   * Private constructor for utility class.
   */
  private IndexTestUtil() {
    // empty
  }

  /**
   * Generate a collection of termData for testing. A Tuple4 consists of
   * <tt>(DocumentId, term, key, value)</tt>. All fields of this tuple are
   * random generated.
   *
   * @param index DataProvider to generate valid document ids, null to
   * generate random ones
   * @param amount Number of test items to create
   * @return Collection of test data items
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not
   * be encoded to target charset
   */
  public static Collection<Tuple.Tuple4<
        Integer, ByteArray, String, Integer>> generateTermData(
          final IndexDataProvider index, final int amount) throws
          UnsupportedEncodingException {
    return generateTermData(index, null, null, amount);
  }

  /**
   * Generate a collection of termData for testing. A Tuple4 consists of
   * <tt>(DocumentId, term, key, value)</tt>. All fields of this tuple are
   * random generated.
   *
   * @param index DataProvider to generate valid document ids, null to
   * generate random ones
   * @param key Key to identify the data
   * @param amount Number of test items to create
   * @return Collection of test data items
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not
   * be encoded to target charset
   */
  public static Collection<Tuple.Tuple4<
        Integer, ByteArray, String, Integer>> generateTermData(
          final IndexDataProvider index, final String key, final int amount)
          throws UnsupportedEncodingException {
    return generateTermData(index, null, key, amount);
  }

  /**
   * * Generate a collection of termData for testing. A Tuple4 consists of
   * <tt>(DocumentId, term, key, value)</tt>. All fields of this tuple are
   * random generated, excluding the given key.
   *
   * @param index DataProvider to generate valid document ids, null to
   * generate random ones
   * @param key Number of test items to create. Generated randomly, if null.
   * @param documentId Document-id to use. Generated randomly, if null.
   * @param amount Amount of entries to generate
   * @return Collection of test data items
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not
   * be encoded to target charset
   */
  public static Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>>
          generateTermData(final IndexDataProvider index,
                  final Integer documentId, String key, final int amount)
          throws UnsupportedEncodingException {
    if (key == null) {
      key = RandomValue.getString(1, 5);
    }
    final Collection<Tuple.Tuple3<Integer, ByteArray, String>> unique
            = new HashSet<>(amount); // ensure unique triples
    final Collection<Tuple.Tuple4<
        Integer, ByteArray, String, Integer>> termData
            = new HashSet<>(amount);
    final int minDocId = 0;
    final int maxDocId;

    if (index == null) {
      maxDocId = RandomValue.getInteger(amount, 1000 + amount);
    } else {
      maxDocId = (int) index.getDocumentCount() - 1;
    }

    for (int i = 0; i < amount;) {
      int docId;
      if (documentId == null) {
        docId = RandomValue.getInteger(minDocId, maxDocId);
      } else {
        docId = documentId;
      }
      final ByteArray term = new ByteArray(RandomValue.getString(1, 20).
              getBytes("UTF-8"));
      final int val = RandomValue.getInteger();
      if (unique.add(Tuple.tuple3(docId, term, key)) && termData.add(Tuple.
              tuple4(docId, term, key, val))) {
        i++; // ensure only unique items are added
      }
    }
    return termData;
  }

  /**
   * Processing target to fill an {@link IndexDataProvider} instance with
   * test-termData.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class IndexTermDataTarget extends Target<Tuple.Tuple4<
        Integer, ByteArray, String, Integer>> {

    private static IndexDataProvider dataTarget;
    private static String prefix;

    /**
     * Initialize the target.
     *
     * @param newSource Source to use
     * @param newDataTarget Target {@link IndexDataProvider}
     * @param newPrefix Prefix to use for adding data
     */
    public IndexTermDataTarget(
            final Source<Tuple.Tuple4<
                    Integer, ByteArray, String, Integer>> newSource,
            final IndexDataProvider newDataTarget,
            final String newPrefix) {
      super(newSource);
      throw new UnsupportedOperationException("BROKEN");
//      dataTarget = newDataTarget;
//      prefix = newPrefix;
    }

    /**
     * Factory instance creator
     *
     * @param newSource Source to use
     */
    private IndexTermDataTarget(
            final Source<Tuple.Tuple4<
                    Integer, ByteArray, String, Integer>> newSource) {
      super(newSource);
    }

    @Override
    public Target<Tuple.Tuple4<
        Integer, ByteArray, String, Integer>> newInstance() {
      return new IndexTermDataTarget(this.getSource());
    }

    @Override
    public void runProcess() throws Exception {
//      while (!isTerminating()) {
//        Tuple.Tuple4<Integer, ByteArray, String, Integer> t4;
//        try {
//          t4 = getSource().next();
//        } catch (ProcessingException.SourceHasFinishedException ex) {
//          break;
//        }
//        if (t4 != null) {
//          if (dataTarget.setTermData(prefix, t4.a, t4.b, t4.c, t4.d) != null) {
//            LOG.warn("A termData value was already set.");
//          }
//        }
//      }
    }

  }
}
