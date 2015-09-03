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

package de.unihildesheim.iw.lucene.index.termfilter;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
@SuppressWarnings("PublicInnerClass")
public final class HeuristicTermFilter {
  public enum MatchCount {
    /**
     * Pattern matching [a-zA-Z] (Unicode).
     */
    ALPHA("\\p{Alpha}"),
    /**
     * Pattern matching [a-zA-Z0-9] (Unicode).
     */
    ALPHANUM("\\p{Alnum}"),
    /**
     * Pattern matching basic ASCII characters ([\x00-\x7F]).
     */
    ASCII("\\p{ASCII}"),
    /**
     * Pattern matching [0-9] (Unicode).
     */
    DIGIT("\\p{Digit}"),
    /**
     * Pattern matching greek characters.
     */
    GREEK("\\p{InGreek}"),
    /**
     * Pattern matching [!"#$%&'()*+,\-./:;<=>?@[\\\]^_`{|}~] (Unicode).
     */
    PUNCT("\\p{Punct}");

    /**
     * Compiled Pattern.
     */
    private final Pattern p;

    MatchCount(final String expression) {
      this.p = Pattern.compile(expression,
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    }

    public int get(@NotNull final CharSequence t) {
      final Matcher m = this.p.matcher(t);
      int count = 0;

      while (m.find()) {
        count++;
      }
      return count;
    }
  }

  /**
   * Filters terms starting with a specific character class.
   */
  public static final class StartsWith
  extends TermFilter {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(StartsWith.class);

    public enum Type {
      DIGIT, PUNCT, ALPHA, ALPHANUM
    }

    private final Pattern p;
    private final Type t;

    public StartsWith(@NotNull final Type type) {
      this.t = type;
      final StringBuilder sb = new StringBuilder("^");
      switch(type) {
        case DIGIT:
          sb.append("\\p{Digit}");
          break;
        case PUNCT:
          sb.append("\\p{Punct}");
          break;
        case ALPHA:
          sb.append("\\p{Alpha}");
          break;
        case ALPHANUM:
          sb.append("\\p{Alnum}");
          break;
      }
      sb.append(".*$");
      this.p = Pattern.compile(sb.toString());
    }

    @Override
    public boolean isAccepted(@Nullable final TermsEnum termsEnum,
        @NotNull final BytesRef term) {
      final String termStr = term.utf8ToString();
      final Matcher m = this.p.matcher(termStr);

      if (LOG.isTraceEnabled() && m.matches()) {
        LOG.trace("Match: t={} type={}", termStr, this.t.name());
      }

      return !m.matches();
    }
  }

  /**
   * Filters terms with repeated characters.
   */
  public static final class Repetition
      extends TermFilter {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(Repetition.class);
    private final Pattern p;
    private final int repCount;

    public Repetition(final int repCount) {
      if (repCount <= 1) {
        throw new IllegalArgumentException("RepCount must be > 1");
      }
      this.repCount = repCount;
      // first is match, so substract one
      this.p = Pattern.compile(".*(\\w)\\1{" + (repCount - 1) + "}.*");
    }

    @Override
    public boolean isAccepted(@Nullable final TermsEnum termsEnum,
        @NotNull final BytesRef term) {
      final String termStr = term.utf8ToString();
      final Matcher m = this.p.matcher(termStr);

      if (LOG.isTraceEnabled() && m.matches()) {
        LOG.trace("Match: t={} rep={}", termStr, this.repCount);
      }

      return !m.matches();
    }
  }

  /**
   * Filters any term having lower than a specified number of alpha [a-Z]
   * characters.
   */
  public static final class AlphaCount
      extends TermFilter {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(AlphaCount.class);

    private final int threshold;

    /**
     * Filters any term having lower than a specified number of alpha [a-Z]
     * characters.
     *
     * @param threshold Number of characters required
     */
    public AlphaCount(final int threshold) {
      this.threshold = threshold;
    }

    @Override
    public boolean isAccepted(@Nullable final TermsEnum termsEnum,
        @NotNull final BytesRef term) {
      final String termStr = term.utf8ToString();
      final int aCount = MatchCount.ALPHA.get(termStr);
      if (LOG.isTraceEnabled() && aCount < this.threshold) {
        LOG.trace("Match: t={} alphaCount={}", termStr, aCount);
      }
      return aCount >= this.threshold;
    }
  }

  /**
   * Try to filter terms that look like they are numbers ending in an any
   * SI-like unit.
   */
  public static final class NumberWithUnit
      extends TermFilter {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(NumberWithUnit.class);
    /**
     * Ratio of digits in relation to other characters.
     */
    private final double digitRatio;
    /**
     * If true, only characters matching [a-Z] of the string will be used for
     * calculating the relation.
     */
    final boolean checkAlphaCharsOnly;

    /**
     * SI-like units matcher pattern.
     */
    private static final Pattern unitPattern = Pattern.compile(
        ".*\\d(?:[acdefghkmnptyzµ]?(?:a|b(?:ar|q)|cd?|f|gy|hz?|j|k(?:at|g)" +
            "?|l[3mx]?|m[23]?|mol|n|pa|rad|s[rv]?|v|wb?|t(?:orr)|°[cfk]?|Ω)" +
            "|fach|molar|(?:milli)?met(?:er)?|inch|volt|zoll)$");

    public NumberWithUnit(final double digitRatio) {
      this.digitRatio = digitRatio;
      this.checkAlphaCharsOnly = true;
    }

    /**
     * @param digitRatio
     * @param alphaOnly If true, only alpha (a-Z) characters of the string will
     * be used for calculating the relation
     */
    public NumberWithUnit(final double digitRatio, final boolean alphaOnly) {
      this.checkAlphaCharsOnly = alphaOnly;
      this.digitRatio = digitRatio;
    }

    @Override
    public boolean isAccepted(@Nullable final TermsEnum termsEnum,
        @NotNull final BytesRef term) {
      final String termStr = term.utf8ToString();
      final int count = MatchCount.DIGIT.get(termStr);

      final int termStrLen;
      if (this.checkAlphaCharsOnly) {
        termStrLen = MatchCount.ALPHA.get(termStr);
      } else {
        termStrLen = termStr.length();
      }

      final double dRatio = ((double) count / (double) termStrLen);

      // check if digit ratio is exceeded
      if (dRatio >= this.digitRatio &&
          unitPattern.matcher(termStr).matches()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Match: t={} ratio={}", termStr, dRatio);
        }
        return false;
      }

      return true;
    }
  }

  /**
   * Filters any term containing a greek character.
   */
  public static final class ContainsGreek
      extends TermFilter {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(ContainsGreek.class);

    @Override
    public boolean isAccepted(@Nullable final TermsEnum termsEnum,
        @NotNull final BytesRef term) {
      final String termStr = term.utf8ToString();

      final boolean contains = MatchCount.GREEK.get(termStr) != 0;

      if (contains && LOG.isTraceEnabled()) {
        LOG.trace("Match: t={}", termStr);
      }

      return !contains;
    }
  }

  /**
   * Excludes terms exceeding a given number of non ASCII characters in relation
   * to the full string length.
   */
  public static final class NonASCIIThreshold
      extends TermFilter {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(NonASCIIThreshold.class);
    final double threshold;

    public NonASCIIThreshold(final double t) {
      this.threshold = t;
    }

    @Override
    public boolean isAccepted(@Nullable final TermsEnum termsEnum,
        @NotNull final BytesRef term) {
      final String termStr = term.utf8ToString();
      final int termStrLen = termStr.length();

      final int countASCII = MatchCount.ASCII.get(termStr);
      final int count = termStrLen - countASCII;

      final double ratio = ((double) count / (double) termStrLen);

      if (LOG.isTraceEnabled() && ratio > this.threshold) {
        LOG.trace("Match: t={} ratio={}", termStr, ratio);
      }

      return ratio <= this.threshold;
    }
  }

  /**
   * Excludes terms exceeding a given number of digits in relation to the full
   * string length.
   */
  public static final class DigitThreshold
      extends TermFilter {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(DigitThreshold.class);
    final double threshold;
    /**
     * If true, only characters matching [a-Z] of the string will be used for
     * calculating the relation.
     */
    final boolean checkAlphaCharsOnly;

    /**
     * Excludes terms exceeding a given number of digits in relation to the full
     * string length.
     *
     * @param t
     */
    public DigitThreshold(final double t) {
      this.checkAlphaCharsOnly = false;
      this.threshold = t;
    }

    /**
     * Excludes terms exceeding a given number of digits in relation to the full
     * string length.
     *
     * @param t String to check
     * @param alphaOnly If true, only alpha (a-Z) characters of the string will
     * be used for calculating the relation
     */
    public DigitThreshold(final double t, final boolean alphaOnly) {
      this.checkAlphaCharsOnly = alphaOnly;
      this.threshold = t;
    }

    @Override
    public boolean isAccepted(@Nullable final TermsEnum termsEnum,
        @NotNull final BytesRef term) {
      final String termStr = term.utf8ToString();
      final int count = MatchCount.DIGIT.get(termStr);

      final int termStrLen;
      if (this.checkAlphaCharsOnly) {
        termStrLen = MatchCount.ALPHA.get(termStr);
      } else {
        termStrLen = termStr.length();
      }

      final double ratio = ((double) count / (double) termStrLen);

      if (LOG.isTraceEnabled() && ratio > this.threshold) {
        LOG.trace("Match: t={} ratio={}", termStr, ratio);
      }

      return ratio <= this.threshold;
    }
  }
}
