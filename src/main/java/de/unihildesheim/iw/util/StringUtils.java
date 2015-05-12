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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
  public static String join(
      @NotNull final String[] strings,
      @NotNull final String separator) {
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
  public static String join(
      @NotNull final Collection<String> strings,
      @NotNull final String separator) {
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
  public static Collection<String> split(
      @NotNull final String str,
      @NotNull final String separator) {
    if (str.isEmpty() || str.length() <= 1) {
      return Collections.singletonList(str);
    }
    return Arrays.asList(str.split(separator));
  }

  /**
   * Manual upper-case function that works on character level to avoid locale
   * problems.
   *
   * @param input String to convert to all lower-case
   * @return Lower-cased input String or plain input String, if empty
   */
  public static String upperCase(@NotNull final CharSequence input) {
    if (isStrippedEmpty(input) || isAllUpper(input)) {
      return input.toString();
    }
    final StringBuilder sb = new StringBuilder(input.length());
    // manual transform to lowercase to avoid locale problems
    input.codePoints().forEach(cp -> sb.append(
        (char) Character.toUpperCase(cp)));
    // string is now all lower case
    return sb.toString();
  }

  /**
   * Checks, if a String is empty, if all characters defined by {@link
   * Character#isWhitespace(int)} are removed.
   *
   * @param input String to check
   * @return True, if String will be empty after stripping those characters or
   * string was initially {@code null}.
   */
  public static boolean isStrippedEmpty(@Nullable final CharSequence input) {
    return input == null ||
        input.length() == 0 ||
        isTrimmedEmpty(input) ||
        !input.codePoints()
            .filter(cp -> !Character.isWhitespace(cp))
            .findFirst().isPresent();

  }

  /**
   * Checks, if all letters of a string are upper-case.
   *
   * @param input String to check
   * @return True, if all letters are upper-case or input String was empty
   */
  public static boolean isAllUpper(final CharSequence input) {
    return isStrippedEmpty(input) ||
        !input.codePoints()
            .filter(cp -> Character.isLetter(cp) && !Character.isUpperCase(cp))
            .findFirst().isPresent();

  }

  /**
   * Checks, if a String is empty, if all characters defined by {@link
   * String#trim()} are removed.
   *
   * @param input String to check
   * @return True, if String will be empty after stripping those characters
   */
  public static boolean isTrimmedEmpty(final CharSequence input) {
    return !input.chars().filter(c -> c > (int) ' ')
        .findFirst().isPresent();
  }

  /**
   * Roughly estimates the number of words in a string by counting the number of
   * whitespaces.
   *
   * @param s String
   * @return Estimated number of words
   */
  @SuppressWarnings({"AssignmentToForLoopParameter", "StatementWithEmptyBody"})
  public static int estimatedWordCount(final CharSequence s) {
    final int chars = s.length();
    int lastIdx = -1;
    int count = 1;

    for (int i = 0; i < chars; i++) {
      if (Character.isWhitespace(s.charAt(i))) {
        if (i > lastIdx + 1) {
          count++;
        }
        lastIdx = i;
      }
    }
    return count;
  }

  /**
   * Counts the occurrence of words in the given string. <br> Based on:
   * http://tutorials.jenkov .com/java-internationalization/breakiterator
   * .html#word-boundaries
   *
   * @param text String to extract words from
   * @param locale Locale to use
   * @return Mapping of (all lower-cased) string and count
   */
  public static Map<String, Integer> countWords(
      @NotNull final String text,
      @NotNull final Locale locale) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<String, Integer> wordCounts = new HashMap<>();

    // short circuit, if string is empty
    if (isStrippedEmpty(text)) {
      return wordCounts;
    }

    final BreakIterator breakIterator = BreakIterator.getWordInstance(locale);
    breakIterator.setText(text);

    int wordBoundaryIndex = breakIterator.first();
    int prevIndex = 0;
    while (wordBoundaryIndex != BreakIterator.DONE) {
      final String word = lowerCase(text.substring(prevIndex,
          wordBoundaryIndex));
      if (isWord(word)) {
        @Nullable Integer wordCount = wordCounts.get(word);
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
   * Manual lower-case function that works on character level to avoid locale
   * problems.
   *
   * @param input String to convert to all lower-case
   * @return Lower-cased input string
   */

  public static String lowerCase(@NotNull final CharSequence input) {
    if (isStrippedEmpty(input) || isAllLower(input)) {
      return input.toString();
    }
    final StringBuilder sb = new StringBuilder(input.length());
    // manual transform to lowercase to avoid locale problems
    input.codePoints().forEach(cp -> sb.append(
        (char) Character.toLowerCase(cp)));
    // string is now all lower case
    return sb.toString();
  }

  /**
   * Checks, if a given string is a letter or number or a character representing
   * something else (digit, semicolon, quote,..)
   *
   * @param word Word to check
   * @return True, if it's a character or number
   */
  @Contract("null -> false")
  private static boolean isWord(@Nullable final CharSequence word) {
    if (word == null) {
      return false;
    }

    if (word.length() == 1) {
      return Character.isLetterOrDigit(word.charAt(0));
    }

    return !isStrippedEmpty(word);
  }

  /**
   * Checks, if all letters of a string are lower-case.
   *
   * @param input String to check
   * @return True, if all letters are lower-case or input String was empty
   */
  public static boolean isAllLower(final CharSequence input) {
    return isStrippedEmpty(input) ||
        !input.codePoints()
            .filter(cp -> Character.isLetter(cp) && !Character.isLowerCase(cp))
            .findFirst().isPresent();
  }

  /**
   * Checks, if a string looks like it's all numeric.
   *
   * @param input String to check
   * @return True, if string look all numeric
   */
  @SuppressWarnings("ImplicitNumericConversion")
  public static boolean isNumeric(final CharSequence input) {
    boolean isNumeric = true;
    for (int i = input.length() - 1; i >= 0 && isNumeric; i--) {
      char c = input.charAt(i);
      isNumeric = (c >= '0' && c <= '9') ||
          c == '-' || c == '.' || c == 'e' || c == 'e';
    }
    return isNumeric;
  }
}
