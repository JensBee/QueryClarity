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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Simple file reader to build a list of stopwords.
 *
 * @author Jens Bertram
 */
public final class StopwordsFileReader {

  /**
   * Whitespace split pattern.
   */
  private static final Pattern WS_SPLIT = Pattern.compile(" ");

  /**
   * Private empty constructor for utility class.
   */
  private StopwordsFileReader() {
  }

  /**
   * Reads stopwords from a file using a defined format.
   *
   * @param format Format to expect
   * @param source Source file path
   * @param cs Charset
   * @return Set of stopwords extracted from the given file
   * @throws IOException Thrown on  low-level I/O errors
   */
  public static Set<String> readWords(
      @NotNull final Format format,
      @NotNull final String source,
      @NotNull final Charset cs)
      throws IOException {
    if (StringUtils.isStrippedEmpty(source)) {
      throw new IllegalArgumentException("Empty source.");
    }

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Set<String> words = new HashSet<>();

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(source), cs))) {

      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();

        // ignore empty lines
        if (line.isEmpty()) {
          continue;
        }

        // skip snowball comment lines
        if (Format.SNOWBALL == format && line.charAt(0) == '|') {
          continue;
        }

        // add the first word
        words.add(WS_SPLIT.split(line, 2)[0]);
      }
    }
    return words;
  }

  /**
   * Tries to get the format type from the given string.
   *
   * @param format String naming the format
   * @return Format or null, if none is matching
   */
  @Nullable
  public static Format getFormatFromString(final String format) {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(format, "Format was null."))) {
      throw new IllegalArgumentException("Format type string was empty.");
    }
    if (Format.PLAIN.name().equalsIgnoreCase(format)) {
      return Format.PLAIN;
    }
    if (Format.SNOWBALL.name().equalsIgnoreCase(format)) {
      return Format.SNOWBALL;
    }
    return null;
  }

  /**
   * Known stopword file formats.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum Format {
    /**
     * Plain file with one word per line.
     */
    PLAIN,
    /**
     * Snowball format. Comments starting with | (pipe).
     */
    SNOWBALL
  }
}
