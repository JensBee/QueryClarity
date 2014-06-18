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

import java.util.Random;

/**
 * Utility functions to create random values.
 *
 * @author Jens Bertram
 */
public final class RandomValue {

  /**
   * Randomized number generator.
   */
  private static final Random RANDOM = new Random();
  /**
   * Character source for random query term generation.
   */
  private static final char[] RAND_TERM_LETTERS = {'a', 'b', 'c',
      'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
      'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5',
      '6', '7', '8', '9', '0'};

  /**
   * Private constructor for utility class.
   */
  private RandomValue() {
    // empty
  }

  /**
   * Get a random int value.
   *
   * @return Random int
   */
  public static int getInteger() {
    return RANDOM.nextInt();
  }

  /**
   * Get a random byte value.
   *
   * @return Random byte
   */
  public static byte getByte() {
    return (byte) getInteger((int) Byte.MIN_VALUE, (int) Byte.MAX_VALUE);
  }

  /**
   * Returns a pseudo-random number between min and max, inclusive. The
   * difference between min and max can be at most <code>Integer.MAX_VALUE -
   * 1</code>. <br> https://stackoverflow.com/a/363692
   *
   * @param min Minimum value
   * @param max Maximum value. Must be greater than min.
   * @return Integer between min and max, inclusive.
   */
  public static int getInteger(final int min, final int max) {
    // nextInt is normally exclusive of the top value,
    // so add 1 to make it inclusive
    return RANDOM.nextInt((max - min) + 1) + min;
  }

  /**
   * Returns a pseudo-random number between min and max, inclusive. The
   * difference between min and max can be at most <code>Double.MAX_VALUE -
   * 1</code>. <br> https://stackoverflow.com/a/363692
   *
   * @param min Minimum value
   * @param max Maximum value. Must be greater than min.
   * @return Double between min and max, inclusive.
   */
  public static double getDouble(final double min, final double max) {
    return min + 1d + (max - min) * RANDOM.nextDouble();
  }

  /**
   * Get a random double value.
   *
   * @return Double value
   */
  public static double getDouble() {
    return RANDOM.nextDouble();
  }

  /**
   * Get a random long value.
   *
   * @return Long value
   */
  public static long getLong() {
    return RANDOM.nextLong();
  }

  /**
   * Get a random long value.
   *
   * @return Long value
   */
  public static long getLong(final long min, final long max) {
    return min + 1L + (max - min) * (long) RANDOM.nextDouble();
  }

  /**
   * Generate a random string based on the list of chars {@link
   * #RAND_TERM_LETTERS} allowed for random term generation.
   *
   * @param minLength Minimum length of the string
   * @param maxLength Maximum length of the string
   * @return A random generated string
   */
  public static String getString(final int minLength,
      final int maxLength) {
    return getString(getInteger(minLength, maxLength));
  }

  /**
   * Generate a random string based on the list of chars {@link
   * #RAND_TERM_LETTERS} allowed for random term generation.
   *
   * @param length Length of the string
   * @return A random generated string
   */
  public static String getString(final int length) {
    final StringBuilder termSb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      termSb.append(getCharacter());
    }
    return termSb.toString();
  }

  /**
   * Get a random char from the list of chars {@link #RAND_TERM_LETTERS} allowed
   * for random term generation.
   *
   * @return a random chosen char
   */
  public static char getCharacter() {
    return RAND_TERM_LETTERS[getInteger(0, RAND_TERM_LETTERS.length - 1)];
  }

  /**
   * Get a random boolean value.
   *
   * @return Random boolean value
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
  public static boolean getBoolean() {
    return RANDOM.nextBoolean();
  }
}
