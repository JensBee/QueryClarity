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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Test for {@link FrenchAnalyzer}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class FrenchAnalyzerTest
    extends TestCase {
  public FrenchAnalyzerTest() {
    super(LoggerFactory.getLogger(FrenchAnalyzerTest.class));
  }

  @SuppressWarnings({"resource", "ObjectAllocationInLoop",
      "ImplicitNumericConversion"})
  @Test
  public void testTokenStream_noStopwords()
      throws Exception {
    final String query = "foo bar baz bam";
    final Analyzer analyzer = new FrenchAnalyzer();
    final BytesRefArray result = new BytesRefArray(Counter.newCounter(false));

    try (TokenStream stream = analyzer.tokenStream(null, query)) {
      stream.reset();
      while (stream.incrementToken()) {
        final BytesRef term = new BytesRef(
            stream.getAttribute(CharTermAttribute.class));
        if (term.length > 0) {
          result.append(term);
        }
      }
    }

    Assert.assertEquals("Not all terms returned.", 4L, result.size());

    final BytesRefIterator bri = result.iterator();
    BytesRef term;
    while ((term = bri.next()) != null) {
      Assert.assertTrue("Unknown term found.",
          "foo".equals(term.utf8ToString()) ||
              "bar".equals(term.utf8ToString()) ||
              "baz".equals(term.utf8ToString()) ||
              "bam".equals(term.utf8ToString()));
    }
  }

  @SuppressWarnings({"resource", "ObjectAllocationInLoop",
      "ImplicitNumericConversion"})
  @Test
  public void testTokenStream()
      throws Exception {
    final CharArraySet csa = new CharArraySet(
        Arrays.asList("foo", "bar"), true);
    final String query = "foo bar baz bam";
    final Analyzer analyzer = new FrenchAnalyzer(csa);
    final BytesRefArray result = new BytesRefArray(Counter.newCounter(false));

    try (TokenStream stream = analyzer.tokenStream(null, query)) {
      stream.reset();
      while (stream.incrementToken()) {
        final BytesRef term = new BytesRef(
            stream.getAttribute(CharTermAttribute.class));
        if (term.length > 0) {
          result.append(term);
        }
      }
    }

    Assert.assertEquals("Not all terms returned.", 2L, result.size());

    final BytesRefIterator bri = result.iterator();
    BytesRef term;
    while ((term = bri.next()) != null) {
      Assert.assertTrue("Unknown term found.",
          "baz".equals(term.utf8ToString()) ||
              "bam".equals(term.utf8ToString()));
    }
  }

  @SuppressWarnings({"resource", "ObjectAllocationInLoop",
      "ImplicitNumericConversion"})
  @Test
  public void testTokenStream_elisions()
      throws Exception {
    final CharArraySet csa = new CharArraySet(
        Arrays.asList("foo", "bar"), true);
    final StringBuilder query = new StringBuilder("foo bar baz bam ");
    // add all elisions to the query
    for (final String s : FrenchAnalyzer.DEFAULT_ELISIONS) {
      query.append(s).append("\'bim ");
    }
    final Analyzer analyzer = new FrenchAnalyzer(csa);
    final BytesRefArray result = new BytesRefArray(Counter.newCounter(false));

    try (TokenStream stream = analyzer.tokenStream(null, query.toString())) {
      stream.reset();
      while (stream.incrementToken()) {
        final BytesRef term = new BytesRef(
            stream.getAttribute(CharTermAttribute.class));
        if (term.length > 0) {
          result.append(term);
        }
      }
    }

    Assert.assertEquals("Not all terms returned.",
        2L + FrenchAnalyzer.DEFAULT_ELISIONS.length, result.size());

    final BytesRefIterator bri = result.iterator();
    BytesRef term;
    while ((term = bri.next()) != null) {
      Assert.assertTrue("Unknown term found.",
          "baz".equals(term.utf8ToString()) ||
              "bam".equals(term.utf8ToString()) ||
                  // elisions should be removed from this
                  "bim".equals(term.utf8ToString()));
    }
  }
}