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

import java.util.Random;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class RandomValue {

  /**
   * Private constructor for utility class.
   */
  private RandomValue() {
    // empty
  }

  /**
   * Randomized number generator.
   */
  private static final Random rand = new Random();

  /**
   * Character source for random query term generation.
   */
  private static final char[] RAND_TERM_LETTERS = new char[]{'a', 'b', 'c',
    'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
    's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', '0'};

  /**
   * Returns a pseudo-random number between min and max, inclusive. The
   * difference between min and max can be at most
   * <code>Integer.MAX_VALUE - 1</code>.
   * <p>
   * https://stackoverflow.com/a/363692
   *
   * @param min Minimum value
   * @param max Maximum value. Must be greater than min.
   * @return Integer between min and max, inclusive.
   */
  public static int getInteger(final int min, final int max) {
    // nextInt is normally exclusive of the top value,
    // so add 1 to make it inclusive
    int randomNum = rand.nextInt((max - min) + 1) + min;

    return randomNum;
  }

  /**
   * Get a random int value.
   *
   * @return Random int
   */
  public static int getInteger() {
    return rand.nextInt();
  }

  /**
   * Returns a pseudo-random number between min and max, inclusive. The
   * difference between min and max can be at most
   * <code>Double.MAX_VALUE - 1</code>.
   * <p>
   * https://stackoverflow.com/a/363692
   *
   * @param min Minimum value
   * @param max Maximum value. Must be greater than min.
   * @return Double between min and max, inclusive.
   */
  public static double getDouble(final double min, final double max) {
    return min + 1 + (max - min) * rand.nextDouble();
  }

  /**
   * Get a random double value.
   * @return Double value
   */
  public static double getDouble() {
    return rand.nextDouble();
  }

  /**
   * Get a random char from the list of chars {@link #RAND_TERM_LETTERS}
   * allowed for random term generation.
   *
   * @return a random chosen char
   */
  public static char getCharacter() {
    final int idx = (int) (rand.nextDouble() * RAND_TERM_LETTERS.length);
    return RAND_TERM_LETTERS[idx];
  }

  /**
   * Generate a random string based on the list of chars
   * {@link #RAND_TERM_LETTERS} allowed for random term generation.
   *
   * @param minLength Minimum length of the string
   * @param maxLength Maximum length of the string
   * @return A random generated string
   *
   */
  public static String getString(final int minLength,
          final int maxLength) {
    return getString(getInteger(minLength, maxLength));
  }

  /**
   * Generate a random string based on the list of chars
   * {@link #RAND_TERM_LETTERS} allowed for random term generation.
   *
   * @param length Length of the string
   * @return A random generated string
   *
   */
  public static String getString(final int length) {
    final StringBuilder termSb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      termSb.append(getCharacter());
    }
    return termSb.toString();
  }

  /**
   * Get a random boolean value.
   * @return Random boolean value
   */
  public static boolean getBoolean() {
    return rand.nextBoolean();
  }
}
