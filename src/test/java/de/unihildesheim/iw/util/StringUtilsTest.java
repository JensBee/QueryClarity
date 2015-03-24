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

package de.unihildesheim.iw.util;

import de.unihildesheim.iw.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

/**
 * Test for {@link StringUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class StringUtilsTest
    extends TestCase {
  public StringUtilsTest() {
    super(LoggerFactory.getLogger(StringUtilsTest.class));
  }

  @Test
  public void testJoin_array()
      throws Exception {
    final String expected = "foo-bar-baz";
    final String result = StringUtils.join(
        new String[]{"foo", "bar", "baz"}, "-");

    Assert.assertEquals("Joined string mismatch.", expected, result);
  }

  @Test
  public void testJoin_collection()
      throws Exception {
    final String expected = "foo-bar-baz";
    final String result = StringUtils.join(
        Arrays.asList("foo", "bar", "baz"), "-");

    Assert.assertEquals("Joined string mismatch.", expected, result);
  }

  @Test
  public void testSplit()
      throws Exception {
    final Collection<String> expected = Arrays.asList("foo", "bar", "baz");
    final Collection<String> result = StringUtils.split("foo-bar-baz", "-");

    Assert.assertTrue("Splitted string mismatch.",
        result.containsAll(expected));
  }

  @Test
  public void testUpperCase()
      throws Exception {
    final String base = "foobar baz";
    final String expected = "FOOBAR BAZ";
    final String result = StringUtils.upperCase(base);

    Assert.assertEquals("Uppercased string mismatch.", expected, result);
  }

  @Test
  public void testUpperCase_nonLetter()
      throws Exception {
    final String base = "foo, bar-baz";
    final String expected = "FOO, BAR-BAZ";
    final String result = StringUtils.upperCase(base);

    Assert.assertEquals("Uppercased string mismatch.", expected, result);
  }

  @SuppressWarnings("HardcodedLineSeparator")
  @Test
  public void testIsStrippedEmpty()
      throws Exception {
    Assert.assertTrue("Stripping string test failed.",
        StringUtils.isStrippedEmpty(" \t \r \n \f "));
  }

  @Test
  public void testIsAllUpper()
      throws Exception {
    Assert.assertTrue("Uppercased string test failed.",
        StringUtils.isAllUpper("FOOBAR BAZ"));
  }

  @SuppressWarnings("HardcodedLineSeparator")
  @Test
  public void testIsTrimmedEmpty()
      throws Exception {
    Assert.assertTrue("Trimming string test failed.",
        StringUtils.isTrimmedEmpty(" \t  \r \n \f "));
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testCountWords()
      throws Exception {
    final String base = "foo bar baz bam_bim, boo";
Assert.assertEquals("Word count mismatch.", 5L,
    StringUtils.countWords(base, Locale.ENGLISH)
        .values().stream().mapToInt(c -> c).sum());
  }

  @Test
  public void testLowerCase()
      throws Exception {
    final String base = "FOOBAR BAZ";
    final String expected = "foobar baz";
    final String result = StringUtils.lowerCase(base);

    Assert.assertEquals("Lowercased string mismatch.", expected, result);
  }

  @Test
  public void testLowerCase_nonLetter()
      throws Exception {
    final String base = "FOO, BAR-BAZ";
    final String expected = "foo, bar-baz";
    final String result = StringUtils.lowerCase(base);

    Assert.assertEquals("Lowercased string mismatch.", expected, result);
  }

  @Test
  public void testIsAllLower()
      throws Exception {
    Assert.assertTrue("Lowercased string test failed.",
        StringUtils.isAllLower("foobar baz"));
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testEstimatedWordCount() {
    final String str = "foo bar baz\tbam   bim";
    final int result = StringUtils.estimatedWordCount(str);

    Assert.assertEquals("Estimated word count mismatch.", 5L, result);
  }
}