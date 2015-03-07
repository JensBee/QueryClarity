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

package de.unihildesheim.iw.lucene.analyzer;

import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.jetbrains.annotations.Nullable;

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
    LANG_LIST = new ArrayList<>(Language.values().length);
    for (final Language lang : Language.values()) {
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
  @Nullable
  public static Language getLanguage(final String lang) {
    if (hasAnalyzer(lang)) {
      return Language.valueOf(StringUtils.upperCase(lang));
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
   * @param stopWords List of stopwords to initialize the Analyzer with
   * @return New Analyzer instance
   */
  @SuppressWarnings("AssignmentToNull")
  public static Analyzer createInstance(@Nullable final Language lang,
      final CharArraySet stopWords) {
    final Analyzer analyzer;
    switch (Objects.requireNonNull(lang, "Language was null.")) {
      case DE:
        analyzer = new GermanAnalyzer(stopWords);
        break;
      case EN:
        analyzer = new EnglishAnalyzer(stopWords);
        break;
      case FR:
        analyzer = new FrenchAnalyzer(stopWords);
        break;
      default:
        // should never be reached
        analyzer = null;
        break;
    }
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
  public static Analyzer createInstance(
      @Nullable final Language lang,
      final IndexDataProvider dataProv) {
    @Nullable final Analyzer analyzer;
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
    return analyzer;
  }

  /**
   * List of known language codes to which an {@link Analyzer} exists.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum Language {
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
    FR;

    /**
     * Try to get a language by it's name.
     *
     * @param lng Language identifier as string
     * @return Language or {@code null} if none was found for the given string
     */
    @Nullable
    public static Language getByString(final String lng) {
      for (final Language srcLng : Language.values()) {
        if (lng.equalsIgnoreCase(srcLng.name())) {
          return srcLng;
        }
      }
      return null;
    }
  }
}
