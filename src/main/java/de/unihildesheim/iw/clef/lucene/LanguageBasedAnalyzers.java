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

package de.unihildesheim.iw.clef.lucene;

import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Jens Bertram
 */
public final class LanguageBasedAnalyzers {

  /**
   * List of known languages.
   */
  private static final List<String> LANG_LIST;

  static {
    LANG_LIST = new ArrayList<>(LanguageAnalyzers.values().length);
    for (final LanguageAnalyzers lang : LanguageAnalyzers.values()) {
      LANG_LIST.add(lang.name());
    }
  }

  /**
   * Private empty constructor for utility class.
   */
  private LanguageBasedAnalyzers() {
  }

  /**
   * Get a language instance for the language described by it's two-char code.
   *
   * @param lang Language code (two-char)
   * @return Language instance
   */
  public static LanguageAnalyzers getLanguage(final String lang) {
    if (hasAnalyzer(lang)) {
      return LanguageAnalyzers.valueOf(StringUtils.upperCase(lang));
    }
    return null;
  }

  /**
   * Check, if an analyzer for the given language exists.
   *
   * @param lang Language code (two-char)
   * @return True, if an analyzer exists
   */
  public static boolean hasAnalyzer(final String lang) {
    return LANG_LIST.contains(StringUtils.upperCase(lang));
  }

  /**
   * Create a new {@link Analyzer} for the provided language.
   *
   * @param lang Language code (two-char)
   * @param matchVersion Lucene version
   * @param stopWords List of stopwords to initialize the Analyzer with
   * @return New Analyzer instance
   */
  @SuppressWarnings("AssignmentToNull")
  public static Analyzer createInstance(final LanguageAnalyzers lang,
      final Version matchVersion, final CharArraySet stopWords) {
    final Analyzer analyzer;
    switch (Objects.requireNonNull(lang, "Language was null.")) {
      case DE:
        analyzer = new GermanAnalyzer(matchVersion, stopWords);
        break;
      case EN:
        analyzer = new EnglishAnalyzer(matchVersion, stopWords);
        break;
      case FR:
        analyzer = new FrenchAnalyzer(matchVersion, stopWords);
        break;
      default:
        // should never be reached
        analyzer = null;
        break;
    }
    assert analyzer != null;
    return analyzer;
  }

  /**
   * Create a new {@link Analyzer} for the provided language using the list of
   * stopwords currently set in the {@link IndexDataProvider}.
   *
   * @param lang Language code (two-char)
   * @param dataProv Provider with stopwords
   * @return New Analyzer instance
   */
  @SuppressWarnings("AssignmentToNull")
  public static Analyzer createInstance(final LanguageAnalyzers lang,
      final IndexDataProvider dataProv) {
    final Analyzer analyzer;
    switch (Objects.requireNonNull(lang, "Language was null.")) {
      case DE:
        analyzer = new GermanAnalyzer(dataProv);
        break;
      case EN:
        analyzer = new EnglishAnalyzer(dataProv);
        break;
      case FR:
        analyzer = new FrenchAnalyzer(dataProv);
        break;
      default:
        // should never be reached
        analyzer = null;
        break;
    }
    assert analyzer != null;
    return analyzer;
  }

  /**
   * List of known language codes to which an {@link Analyzer} exists.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum LanguageAnalyzers {
    /**
     * German.
     */
    DE,
    /**
     * English.
     */
    EN,
    /**
     * French.
     */
    FR
  }
}
