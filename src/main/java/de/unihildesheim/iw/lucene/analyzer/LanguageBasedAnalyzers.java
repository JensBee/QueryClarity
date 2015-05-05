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

import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Jens Bertram
 */
public final class LanguageBasedAnalyzers {

  /**
   * List of known languages.
   */
  private static final Set<String> LANG_LIST;

  static {
    LANG_LIST = new HashSet<>(Language.values().length);
    for (final Language lang : Language.values()) {
      LANG_LIST.add(lang.toUpperCaseString());
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
  public static Language getLanguage(final CharSequence lang) {
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
  public static boolean hasAnalyzer(final CharSequence lang) {
    return LANG_LIST.contains(StringUtils.upperCase(lang));
  }

  /**
   * Create a new {@link Analyzer} for the provided language.
   *
   * @param lang Language code (two-char)
   * @param stopWords List of stopwords to initialize the Analyzer with
   * @return New Analyzer instance
   */
  @SuppressWarnings({"AssignmentToNull", "resource"})
  @NotNull
  public static Analyzer createInstance(
      @NotNull final Language lang,
      @Nullable final CharArraySet stopWords) {
    final Analyzer analyzer;
    switch (lang) {
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
   * Create a new {@link Analyzer} for the provided language without using any
   * stopwords.
   *
   * @param lang Language code (two-char)
   * @return New Analyzer instance
   */
  @SuppressWarnings("resource")
  public static Analyzer createInstance(@NotNull final Language lang) {
    return createInstance(lang, null);
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

    /**
     * Get the lower-cased identifier for the current language.
     * @return Lower-cased language identifier
     */
    @Override
    public String toString() {
      return StringUtils.lowerCase(this.name());
    }

    /**
     * Get the upper-cased identifier for the current language.
     * @return Upper-cased language identifier
     */
    public String toUpperCaseString() {
      return StringUtils.upperCase(this.name());
    }
  }
}
