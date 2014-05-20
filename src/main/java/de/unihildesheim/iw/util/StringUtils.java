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
package de.unihildesheim.iw.util;

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class for string operations.
 */
public final class StringUtils {

  /**
   * Private empty constructor for utility class.
   */
  private StringUtils() {
    // empty constructor for utility class
  }

  /**
   * Joins a string array using a given separator string.
   *
   * @param strings Strings to join
   * @param separator Separator char
   * @return Joined string
   */
  public static String join(final String[] strings, final String separator) {
    Objects.requireNonNull(strings, "Strings were null.");
    Objects.requireNonNull(separator, "Separator was null.");

    // estimate final length
    int approxLength = 0;
    for (final String s : strings) {
      approxLength += s.length();
    }
    approxLength += separator.length() * strings.length;

    final StringBuilder joinedStr = new StringBuilder(approxLength);
    for (int i = 0, il = strings.length; i < il; i++) {
      if (i > 0) {
        joinedStr.append(separator);
      }
      joinedStr.append(strings[i]);
    }
    return joinedStr.toString();
  }

  /**
   * Joins a string array using a given separator string.
   *
   * @param strings Strings to join
   * @param separator Separator char
   * @return Joined string
   */
  public static String join(final Collection<String> strings,
      final String separator) {
    Objects.requireNonNull(separator, "Separator was null.");
    Objects.requireNonNull(strings, "Strings were null.");

    // short circuit, if list is empty
    if (strings.isEmpty()) {
      return "";
    }
    final List<String> stringsList = new ArrayList<>(strings);

    // estimate final length
    int approxLength = 0;
    for (final String s : stringsList) {
      approxLength += s.length();
    }
    approxLength += separator.length() * stringsList.size();

    final StringBuilder joinedStr = new StringBuilder(approxLength);
    for (int i = 0, il = stringsList.size(); i < il; i++) {
      if (i > 0) {
        joinedStr.append(separator);
      }
      joinedStr.append(stringsList.get(i));
    }
    return joinedStr.toString();
  }

  /**
   * Splits the given string at the given separator.
   *
   * @param str String to split
   * @param separator Separator to use for splitting
   * @return Collection of splitted string parts
   */
  public static Collection<String> split(final String str,
      final String separator) {
    Objects.requireNonNull(str, "String was null.");
    Objects.requireNonNull(separator, "Seperator was null.");

    if (str.isEmpty() || str.length() <= 1) {
      return Arrays.asList(new String[]{str});
    }
    return Arrays.asList(str.split(separator));
  }

  /**
   * Manual lower-case function that works on character level to avoid locale
   * problems.
   *
   * @param input String to convert to all lower-case
   * @return Lower-cased input string
   */
  public static String lowerCase(final String input) {
    Objects.requireNonNull(input, "String was null.");

    if (input.trim().isEmpty()) {
      return input;
    }
    // manual transform to lowercase to avoid locale problems
    char[] inputChars = input.toCharArray();
    for (int i = 0; i < inputChars.length; i++) {
      inputChars[i] = Character.toLowerCase(inputChars[i]);
    }
    // string is now all lower case
    return new String(inputChars);
  }

  /**
   * Counts the occurrence of words in the given string.
   * <p/>
   * Based on: http://tutorials.jenkov
   * .com/java-internationalization/breakiterator.html#word-boundaries
   *
   * @param text String to extract words from
   * @param locale Locale to use
   * @return Mapping of (all lower-cased) string and count
   */
  public static Map<String, Integer> countWords(final String text,
      final Locale locale) {
    Objects.requireNonNull(text, "String was null.");
    Objects.requireNonNull(locale, "Locale was null.");

    final Map<String, Integer> wordCounts = new HashMap<>();

    // short circuit, if string is empty
    if (text.trim().isEmpty()) {
      return wordCounts;
    }

    final BreakIterator breakIterator = BreakIterator.getWordInstance(locale);
    breakIterator.setText(text);

    int wordBoundaryIndex = breakIterator.first();
    int prevIndex = 0;
    while (wordBoundaryIndex != BreakIterator.DONE) {
      final String word = StringUtils.lowerCase(text.substring(prevIndex,
          wordBoundaryIndex));
      if (isWord(word)) {
        Integer wordCount = wordCounts.get(word);
        if (wordCount == null) {
          wordCount = 0;
        }
        wordCount++;
        wordCounts.put(word, wordCount);
      }
      prevIndex = wordBoundaryIndex;
      wordBoundaryIndex = breakIterator.next();
    }

    return wordCounts;
  }

  /**
   * Checks, if a given string is a letter or number or a character representing
   * something else (digit, semicolon, quote,..)
   *
   * @param word Word to check
   * @return True, if it's a character or number
   */
  private static boolean isWord(final String word) {
    assert word != null;

    if (word.length() == 1) {
      return Character.isLetterOrDigit(word.charAt(0));
    }
    return !"".equals(word.trim());
  }
}
