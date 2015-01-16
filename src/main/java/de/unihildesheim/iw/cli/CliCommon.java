/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

package de.unihildesheim.iw.cli;

import de.unihildesheim.iw.util.StopwordsFileReader;
import de.unihildesheim.iw.util.StopwordsFileReader.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

/**
 * Utility class containing methods often used by various cli classes.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class CliCommon {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(CliCommon.class);

  /**
   * Tries to load a list of stopwords.
   * @param lang Language to load stopwords for. Uses a two-char language code.
   * @param format Stopword file format as specified in {@link
   * StopwordsFileReader.Format}
   * @param pattern File name pattern. Gets suffixated by '_[lang].txt'
   * @return Set of stopwords as strings
   * @throws IOException Thrown, if loading the file fails
   */
  public static Set<String> getStopwords(final String lang, final String
      format, final String pattern)
      throws IOException {
    Set<String> sWords = Collections.emptySet();
    // read stopwords
    final Format stopFileFormat =
        StopwordsFileReader.getFormatFromString(format);
    if (stopFileFormat != null) {
      final File stopFile = new File(pattern + "_" +
          lang + ".txt");
      if (stopFile.exists() && stopFile.isFile()) {
        final Set<String> newWords = StopwordsFileReader.readWords
            (stopFileFormat, stopFile.getCanonicalPath(),
                StandardCharsets.UTF_8);
        if (newWords != null) {
          LOG.info("Loaded {} stopwords. file={} lang={}", newWords.size(),
              stopFile, lang);
          sWords = newWords;
        }
      } else {
        LOG.warn("Stopwords file '{}' not found. No stopwords set. lang={}",
            stopFile, lang);
      }
    }
    return sWords;
  }
}
