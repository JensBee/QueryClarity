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
package de.unihildesheim.lucene.util;

import de.unihildesheim.util.RandomValue;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link BytesWrap}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class BytesWrapTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          BytesWrapTest.class);

  /**
   * Test of clone method, of class BytesWrap.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  public void testClone() throws Exception {
    LOG.info("Test clone");
    final byte[] testBytes = "testString".getBytes("UTF-8");
    final BytesWrap instance = new BytesWrap(testBytes);
    final BytesWrap result = instance.clone();
    assertArrayEquals("Byte array mismatch.", instance.getBytes(), result.
            getBytes());
  }

  /**
   * Test of toString method, of class BytesWrap.
   */
  @Test
  public void testToString() throws Exception {
    LOG.info("Test toString");
    final String testString = "testString";
    final BytesWrap instance = new BytesWrap(testString.getBytes("UTF-8"));
    assertEquals("String mismatch.", testString, instance.toString());
  }

  /**
   * Test of getBytes method, of class BytesWrap.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  public void testGetBytes() throws Exception {
    LOG.info("Test getBytes");
    final String testString = "testString";
    final BytesWrap instance = new BytesWrap(testString.getBytes("UTF-8"));
    final byte[] expResult = testString.getBytes("UTF-8");
    final byte[] result = instance.getBytes();
    assertArrayEquals("Byte array mismatch.", expResult, result);
  }

  /**
   * Test of equals method, of class BytesWrap.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  @java.lang.SuppressWarnings("ObjectEqualsNull")
  public void testEquals() throws Exception {
    LOG.info("Test equals");
    final String testString = "testString";
    final BytesWrap instance = new BytesWrap(testString.getBytes("UTF-8"));
    final BytesWrap otherInstance
            = new BytesWrap(testString.getBytes("UTF-8"));

    assertFalse("Should not be equal null.", instance.equals(null));
    assertFalse("Should not be equal Object.", instance.equals(new Object()));

    assertTrue("Should be equal to it's clone.", instance.equals(instance.
            clone()));
    assertTrue("Should be equal to same String instance.", instance.equals(
            otherInstance));
  }

  /**
   * Test of hashCode method, of class BytesWrap.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  public void testHashCode() throws Exception {
    LOG.info("Test hashCode");
    final String testString = "testString";
    final BytesWrap instance = new BytesWrap(testString.getBytes("UTF-8"));
    final BytesWrap otherInstance
            = new BytesWrap(testString.getBytes("UTF-8"));
    final BytesWrap randInstance = new BytesWrap(RandomValue.getString(10).
            getBytes("UTF-8"));

    assertEquals("HashCode should be the same for same String.", instance.
            hashCode(), otherInstance.hashCode());
    assertNotEquals("HashCode should not be the same for different String.",
            instance.hashCode(), randInstance.hashCode());
  }

  /**
   * Test of compareTo method, of class BytesWrap.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  public void testCompareTo() throws Exception {
    LOG.info("Test compareTo");
    final String lowString = "aaa";
    final String highString = "zzz";
    final BytesWrap lowStrBw = new BytesWrap(lowString.getBytes("UTF-8"));
    final BytesWrap highStrBw = new BytesWrap(highString.getBytes("UTF-8"));
    if (lowStrBw.compareTo(highStrBw) > -1) {
      fail("Failed compare low to high (str). result=" + lowStrBw.compareTo(
              highStrBw));
    }
    if (highStrBw.compareTo(lowStrBw) < 1) {
      fail("Failed compare high to low (str). result=" + highStrBw.compareTo(
              lowStrBw));
    }
    if (highStrBw.compareTo(highStrBw) != 0) {
      fail("Failed compare high to high (str). result=" + highStrBw.compareTo(
              highStrBw));
    }
    if (lowStrBw.compareTo(lowStrBw) != 0) {
      fail("Failed compare low to low (str). result=" + lowStrBw.compareTo(
              lowStrBw));
    }

    final String lowMixString = "1a37";
    final String highMixString = "9sgb";
    final BytesWrap lowMixStrBw = new BytesWrap(lowMixString.getBytes());
    final BytesWrap highMixStrBw = new BytesWrap(highMixString.getBytes());
    if (lowMixStrBw.compareTo(highMixStrBw) > -1) {
      fail("Failed compare low to high (mixStr). result=" + lowMixStrBw.
              compareTo(highMixStrBw));
    }
    if (highMixStrBw.compareTo(lowMixStrBw) < 1) {
      fail("Failed compare high to low (mixStr). result=" + highMixStrBw.
              compareTo(lowMixStrBw));
    }
    if (highMixStrBw.compareTo(highMixStrBw) != 0) {
      fail("Failed compare high to high (mixStr). result=" + highMixStrBw.
              compareTo(highMixStrBw));
    }
    if (lowMixStrBw.compareTo(lowMixStrBw) != 0) {
      fail("Failed compare low to low (mixStr). result=" + lowMixStrBw.
              compareTo(lowMixStrBw));
    }

    final String lowNum = "111";
    final String highNum = "999";
    final BytesWrap lowNumBw = new BytesWrap(lowNum.getBytes());
    final BytesWrap highNumBw = new BytesWrap(highNum.getBytes());
    if (lowNumBw.compareTo(highNumBw) > -1) {
      fail("Failed compare low to high (num). result=" + lowNumBw.compareTo(
              highNumBw));
    }
    if (highNumBw.compareTo(lowNumBw) < 1) {
      fail("Failed compare high to low (num). result=" + highNumBw.compareTo(
              lowNumBw));
    }
    if (highNumBw.compareTo(highNumBw) != 0) {
      fail("Failed compare high to high (num). result=" + highNumBw.compareTo(
              highNumBw));
    }
    if (lowNumBw.compareTo(lowNumBw) != 0) {
      fail("Failed compare low to low (num). result=" + lowNumBw.compareTo(
              lowNumBw));
    }
  }

}
