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
import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.util.RandomValue;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashSet;

/**
 * Utility class for testing referenceIndex related functions.
 *
 * @author Jens Bertram
 */
public final class IndexTestUtils {

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
   * @param index DataProvider to generate valid document ids, null to generate
   * random ones
   * @param amount Number of test items to create
   * @return Collection of test data items
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not be
   * encoded to target charset
   */
  public static Collection<Tuple.Tuple4<
      Integer, ByteArray, String, Integer>> generateTermData(
      final IndexDataProvider index, final int amount)
      throws UnsupportedEncodingException {
    if (amount <= 0) {
      throw new IllegalArgumentException("Amount must be greater than 0.");
    }
    return generateTermData(index, null, null, amount);
  }

  /**
   * * Generate a collection of termData for testing. A Tuple4 consists of
   * <tt>(DocumentId, term, key, value)</tt>. All fields of this tuple are
   * random generated, excluding the given key.
   *
   * @param index DataProvider to generate valid document ids, null to generate
   * random ones
   * @param key Number of test items to create. Generated randomly, if null.
   * @param documentId Document-id to use. Generated randomly, if null.
   * @param amount Amount of entries to generate
   * @return Collection of test data items
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not be
   * encoded to target charset
   */
  public static Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>>
  generateTermData(final IndexDataProvider index,
      final Integer documentId, final String key, final int amount)
      throws UnsupportedEncodingException {
    assert amount > 0;
    final String finalKey;
    if (key == null) {
      finalKey = RandomValue.getString(1, 5);
    } else {
      finalKey = key;
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

    for (int i = 0; i < amount; ) {
      final int docId;
      if (documentId == null) {
        docId = RandomValue.getInteger(minDocId, maxDocId);
      } else {
        docId = documentId;
      }
      @SuppressWarnings("ObjectAllocationInLoop")
      final ByteArray term = new ByteArray(RandomValue.getString(1, 20).
          getBytes("UTF-8"));
      final int val = RandomValue.getInteger();
      if (unique.add(Tuple.tuple3(docId, term, finalKey)) && termData.add(Tuple.
          tuple4(docId, term, finalKey, val))) {
        i++; // ensure only unique items are added
      }
    }
    return termData;
  }

  /**
   * Get a {@link StandardAnalyzer} initialized with the supplied stopwords
   * set.
   *
   * @param stopwords List of stopwords to set
   * @return Analyzer with current stopwords set
   */
  public static Analyzer getAnalyzer(final Collection<String> stopwords) {
    return new StandardAnalyzer(LuceneDefaults.VERSION,
        new CharArraySet(LuceneDefaults.VERSION, stopwords, true)
    );
  }

  /**
   * Get a {@link StandardAnalyzer} without any stopwords set.
   *
   * @return Analyzer
   */
  public static Analyzer getAnalyzer() {
    return new StandardAnalyzer(LuceneDefaults.VERSION, CharArraySet.EMPTY_SET);
  }
}
