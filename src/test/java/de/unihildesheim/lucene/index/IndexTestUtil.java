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
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for testing index related functions.
 *
 * @author Jens Bertram
 */
public final class IndexTestUtil {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          IndexTestUtil.class);

  /**
   * Creates an {@link IndexDataProvider} instance and sets it active in the
   * {@link Environment} using the {@link TestIndexDataProvider}.
   *
   * @param index {
   * @TestIndexDataProvider} to access utility functions
   * @param dataProv {@link IndexDataProvider} class to create
   * @param fields Document fields to use (may be null, to use all)
   * @param stopwords Stopwords to use (may be null, to use none)
   * @return {@link IndexDataProvider} instance
   * @throws Exception Thrown on lower level errors
   */
  public static IndexDataProvider createInstance(
          final TestIndexDataProvider index,
          final Class<? extends IndexDataProvider> dataProv,
          final Collection<String> fields,
          final Collection<String> stopwords) throws Exception {
    Environment.clear();
    index.setupEnvironment(dataProv, fields, stopwords);
    IndexDataProvider instance = Environment.getDataProvider();
    instance.createCache("test");
    instance.warmUp();
    return instance;
  }

  /**
   * Picks some (1 to n) terms from the index.
   *
   * @param dataProv Data provider
   * @return Stop words term collection
   */
  public static Collection<String> getRandomStopWords(
          final TestIndexDataProvider dataProv) {
    Iterator<ByteArray> termsIt = dataProv.getTermsIterator();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<String> stopWords = new ArrayList<>();
    while (termsIt.hasNext()) {
      if (RandomValue.getBoolean()) {
        stopWords.add(ByteArrayUtil.utf8ToString(termsIt.next()));
      } else {
        termsIt.next();
      }
    }
    if (stopWords.isEmpty()) {
      stopWords.add(ByteArrayUtil.utf8ToString(new ArrayList<>(
              dataProv.getTermSet()).get(0)));
    }
    return stopWords;
  }

  /**
   * Get random index fields.
   *
   * @param index {@link TestIndexDataProvider}
   * @param set If true, set fields to {@link Environment}
   * @return List of fields set
   */
  private static Collection<String> getRandomFields(
          final TestIndexDataProvider index, final boolean set) {
    Collection<String> fields = index.getRandomFields();
    return fields;
  }

  /**
   * Get random index fields.
   *
   * @param index {@link TestIndexDataProvider}
   * @return List of fields set
   */
  public static Collection<String> getRandomFields(
          final TestIndexDataProvider index) {
    return getRandomFields(index, true);
  }

  /**
   * Get the list of stopwords currently set in the {@link Environment}.
   *
   * @return Collection of stopwords or <tt>null</tt>, if none are set
   * @throws UnsupportedEncodingException Thrown, if a stopword could not be
   * proper encoded
   */
  public static Collection<ByteArray> getStopwordBytesFromEnvironment() throws
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
