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
package de.unihildesheim.util;

import java.util.Arrays;
import java.util.Collection;

/**
 * Utility class for string operations.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class StringUtils {

  /**
   * Private empty constructor for utility class.
   */
  private StringUtils() {
    // empty constructor for utility class
  }

  /**
   * Join a string array using a given separator string.
   *
   * @param strings Strings to join
   * @param seperator Separator char
   * @return Joined string
   */
  public static String join(final String[] strings, final String seperator) {
    final StringBuilder joinedStr = new StringBuilder(strings.toString().
            length());
    for (int i = 0, il = strings.length; i < il; i++) {
      if (i > 0) {
        joinedStr.append(seperator);
      }
      joinedStr.append(strings[i]);
    }
    return joinedStr.toString();
  }

  public static Collection<String> split(final String str,
          final String seperator) {
    return Arrays.asList(str.split(seperator));
  }

  /**
   * Manual lower-case function that works on character level to avoid locale
   * problems.
   *
   * @param input String to convert to all lower-case
   * @return Lower-cased input string
   */
  public static String lowerCase(final String input) {
    // manual transform to lowercase to avoid locale problems
    char[] inputChars = input.toCharArray();
    for (int i = 0; i < inputChars.length; i++) {
      inputChars[i] = Character.toLowerCase(inputChars[i]);
    }
    // string is now all lower case
    return new String(inputChars);
  }
}
