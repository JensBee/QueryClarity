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

package de.unihildesheim.iw.lucene.analyzer;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers.Language;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Test for {@link LanguageBasedAnalyzers}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class LanguageBasedAnalyzersTest
    extends TestCase {
  public LanguageBasedAnalyzersTest() {
    super(LoggerFactory.getLogger(LanguageBasedAnalyzersTest.class));
  }

  @Test
  public void testGetLanguage()
      throws Exception {
    for (final Language l : Language.values()) {
      final String lang = l.name().toLowerCase();
      // all lower
      Assert.assertEquals("Language mismatch.", l,
          LanguageBasedAnalyzers.getLanguage(lang));
      // first upper
      Assert.assertEquals("Language mismatch.", l,
          LanguageBasedAnalyzers.getLanguage(
              Character.toUpperCase(lang.charAt(0)) +
                  String.valueOf(lang.charAt(1))));
      // last upper
      Assert.assertEquals("Language mismatch.", l,
          LanguageBasedAnalyzers.getLanguage(
              String.valueOf(lang.charAt(0)) +
                  Character.toUpperCase(lang.charAt(1))));
      // all upper
      Assert.assertEquals("Language mismatch.", l,
          LanguageBasedAnalyzers.getLanguage(lang.toUpperCase()));
    }
    Assert.assertNull("Unknown language found.",
        LanguageBasedAnalyzers.getLanguage("foo"));
  }

  @Test
  public void testHasAnalyzer()
      throws Exception {
    for (final Language l : Language.values()) {
      final String lang = l.name().toLowerCase();
      // all lower
      Assert.assertTrue("Language mismatch.",
          LanguageBasedAnalyzers.hasAnalyzer(lang));
      // first upper
      Assert.assertTrue("Language mismatch.",
          LanguageBasedAnalyzers.hasAnalyzer(
              Character.toUpperCase(lang.charAt(0)) +
                  String.valueOf(lang.charAt(1))));
      // last upper
      Assert.assertTrue("Language mismatch.",
          LanguageBasedAnalyzers.hasAnalyzer(
              String.valueOf(lang.charAt(0)) +
                  Character.toUpperCase(lang.charAt(1))));
      // all upper
      Assert.assertTrue("Language mismatch.",
          LanguageBasedAnalyzers.hasAnalyzer(lang.toUpperCase()));
    }
    Assert.assertFalse("Unknown language found.",
        LanguageBasedAnalyzers.hasAnalyzer("foo"));
  }

  @SuppressWarnings("UnnecessaryDefault")
  @Test
  public void testCreateInstance()
      throws Exception {
    final CharArraySet csa = new CharArraySet(
        Arrays.asList("foo", "bar", "baz"), true);
    for (final Language l : Language.values()) {
      final Analyzer instance = LanguageBasedAnalyzers.createInstance(l, csa);
      switch (l) {
        case DE:
          Assert.assertTrue("Wrong Analyzer instance found.", GermanAnalyzer
              .class.isInstance(instance));
          break;
        case EN:
          Assert.assertTrue("Wrong Analyzer instance found.", EnglishAnalyzer
              .class.isInstance(instance));
          break;
        case FR:
          Assert.assertTrue("Wrong Analyzer instance found.", FrenchAnalyzer
              .class.isInstance(instance));
          break;
        default:
          Assert.fail("Test is incomplete. Not all languages covered.");
          break;
      }
    }
  }

  @SuppressWarnings("UnnecessaryDefault")
  @Test
  public void testCreateInstance_noStopwords()
      throws Exception {
    for (final Language l : Language.values()) {
      final Analyzer instance = LanguageBasedAnalyzers.createInstance(l);
      switch (l) {
        case DE:
          Assert.assertTrue("Wrong Analyzer instance found.", GermanAnalyzer
              .class.isInstance(instance));
          break;
        case EN:
          Assert.assertTrue("Wrong Analyzer instance found.", EnglishAnalyzer
              .class.isInstance(instance));
          break;
        case FR:
          Assert.assertTrue("Wrong Analyzer instance found.", FrenchAnalyzer
              .class.isInstance(instance));
          break;
        default:
          Assert.fail("Test is incomplete. Not all languages covered.");
          break;
      }
    }
  }
}