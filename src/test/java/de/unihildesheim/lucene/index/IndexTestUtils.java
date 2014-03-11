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

import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.Tuple;
import de.unihildesheim.util.concurrent.processing.ProcessingException;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.util.Collection;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class IndexTestUtils {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          IndexTestUtils.class);

  /**
   * Private constructor for utility class.
   */
  private IndexTestUtils() {
    // empty
  }

  /**
   * Generate a collection of termData for testing. A Tuple4 consists of
   * <tt>(DocumentId, term, key, value)</tt>. All fields of this tuple are
   * random generated.
   *
   * @param index DataProvider to generate valid document ids
   * @param amount Number of test items to create
   * @return Collection of test data items
   */
  public static Collection<Tuple.Tuple4<
        Integer, BytesWrap, String, Integer>> generateTermData(
          final IndexDataProvider index, final int amount) {
    return generateTermData(index, null, null, amount);
  }

  /**
   * Generate a collection of termData for testing. A Tuple4 consists of
   * <tt>(DocumentId, term, key, value)</tt>. All fields of this tuple are
   * random generated.
   *
   * @param index DataProvider to generate valid document ids
   * @param key Key to identify the data
   * @param amount Number of test items to create
   * @return Collection of test data items
   */
  public static Collection<Tuple.Tuple4<
        Integer, BytesWrap, String, Integer>> generateTermData(
          final IndexDataProvider index, final String key, final int amount) {
    return generateTermData(index, null, key, amount);
  }

  /**
   * * Generate a collection of termData for testing. A Tuple4 consists of
   * <tt>(DocumentId, term, key, value)</tt>. All fields of this tuple are
   * random generated, excluding the given key.
   *
   * @param index DataProvider to generate valid document ids
   * @param key Number of test items to create. Generated randomly, if null.
   * @param documentId Document-id to use. Generated randomly, if null.
   * @param amount Amount of entries to generate
   * @return Collection of test data items
   */
  public static Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>>
          generateTermData(final IndexDataProvider index,
                  final Integer documentId, String key, final int amount) {
    if (key == null) {
      key = RandomValue.getString(1, 5);
    }
    final Collection<Tuple.Tuple3<Integer, BytesWrap, String>> unique
            = new HashSet<>(amount); // ensure unique triples
    final Collection<Tuple.Tuple4<
        Integer, BytesWrap, String, Integer>> termData
            = new HashSet<>(amount);
    final int minDocId = 0;
    final int maxDocId = (int) index.getDocumentCount() - 1;

    for (int i = 0; i < amount;) {
      int docId;
      if (documentId == null) {
        docId = RandomValue.getInteger(minDocId, maxDocId);
      } else {
        docId = documentId;
      }
      final BytesWrap term = new BytesWrap(RandomValue.getString(1, 20).
              getBytes());
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
        Integer, BytesWrap, String, Integer>> {

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
                    Integer, BytesWrap, String, Integer>> newSource,
            final IndexDataProvider newDataTarget,
            final String newPrefix) {
      super(newSource);
      dataTarget = newDataTarget;
      prefix = newPrefix;
    }

    /**
     * Factory instance creator
     *
     * @param newSource Source to use
     */
    private IndexTermDataTarget(
            final Source<Tuple.Tuple4<
                    Integer, BytesWrap, String, Integer>> newSource) {
      super(newSource);
    }

    @Override
    public Target<Tuple.Tuple4<
        Integer, BytesWrap, String, Integer>> newInstance() {
      return new IndexTermDataTarget(this.getSource());
    }

    @Override
    public void runProcess() throws Exception {
      while (!isTerminating()) {
        Tuple.Tuple4<Integer, BytesWrap, String, Integer> t4;
        try {
          t4 = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }
        if (t4 != null) {
          if (dataTarget.setTermData(prefix, t4.a, t4.b, t4.c, t4.d) != null) {
            LOG.warn("A termData value was already set.");
          }
        }
      }
    }

  }
}
