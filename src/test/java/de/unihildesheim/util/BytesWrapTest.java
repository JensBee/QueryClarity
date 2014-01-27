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
package de.unihildesheim.util;

import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.TestUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class BytesWrapTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          BytesWrapTest.class);

  public BytesWrapTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of wrap method, of class BytesWrap.
   */
  @Test
  public void testWrap() {
    TestUtility.logHeader(LOG, "wrap");

    final String testStr1 = "fooBar";
    byte[] existingBytes = testStr1.getBytes();
    BytesWrap expResult = BytesWrap.wrap(testStr1.getBytes());
    BytesWrap result = BytesWrap.wrap(existingBytes);
    assertEquals(expResult, result);

    LOG.debug("origin={} wrap={} - before mod", expResult.getBytes(), result.
            getBytes());

    existingBytes[0] = 0x0;

    LOG.debug("origin={} wrap={} - after mod (should be not equal)", expResult.
            getBytes(), result.getBytes());
    assertNotEquals(expResult.getBytes(), result.getBytes());
  }

  /**
   * Test of duplicate method, of class BytesWrap.
   */
  @Test
  public void testDuplicate_byteArr() {
    TestUtility.logHeader(LOG, "duplicate");

    final String testStr1 = "fooBar";
    byte[] existingBytes = testStr1.getBytes();
    BytesWrap expResult = BytesWrap.wrap(testStr1.getBytes());
    BytesWrap result = BytesWrap.wrap(existingBytes);
    assertEquals(expResult, result);

    LOG.debug("origin={} wrap={} - before mod", expResult.getBytes(), result.
            getBytes());

    result.duplicate();
    existingBytes[0] = 0x0;

    LOG.debug("origin={} wrap={} - after mod (should be equal)", expResult.
            getBytes(), result.getBytes());

    assertEquals(expResult, result);
  }

  /**
   * Test of duplicate method, of class BytesWrap.
   */
  @Test
  @Ignore
  public void testDuplicate_0args() {
    System.out.println("duplicate");
    BytesWrap instance = null;
    BytesWrap expResult = null;
    BytesWrap result = instance.duplicate();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getBytes method, of class BytesWrap.
   */
  @Test
  @Ignore
  public void testGetBytes() {
    System.out.println("getBytes");
    BytesWrap instance = null;
    byte[] expResult = null;
    byte[] result = instance.getBytes();
    assertArrayEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of equals method, of class BytesWrap.
   */
  @Test
  @Ignore
  public void testEquals() {
    System.out.println("equals");
    Object o = null;
    BytesWrap instance = null;
    boolean expResult = false;
    boolean result = instance.equals(o);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of hashCode method, of class BytesWrap.
   */
  @Test
  @Ignore
  public void testHashCode() {
    System.out.println("hashCode");
    BytesWrap instance = null;
    int expResult = 0;
    int result = instance.hashCode();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

}
