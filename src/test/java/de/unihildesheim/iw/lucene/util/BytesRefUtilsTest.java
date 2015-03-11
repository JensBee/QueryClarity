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

package de.unihildesheim.iw.lucene.util;

import de.unihildesheim.iw.TestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Test for {@link BytesRefUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class BytesRefUtilsTest
    extends TestCase {
  public BytesRefUtilsTest() {
    super(LoggerFactory.getLogger(BytesRefUtilsTest.class));
  }

  @Test
  public void testCopyBytes()
      throws Exception {
    final BytesRef br = new BytesRef("foo");
    final BytesRef result = new BytesRef(BytesRefUtils.copyBytes(br));
    Assert.assertTrue("Bytes mismatch.", br.bytesEquals(result));
  }

  @Test
  public void testCopyBytes_empty()
      throws Exception {
    final BytesRef br = new BytesRef();
    final BytesRef result = new BytesRef(BytesRefUtils.copyBytes(br));
    Assert.assertTrue("Bytes mismatch.", br.bytesEquals(result));
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testHashToArray()
      throws Exception {
    final Collection<String> data = new HashSet<>(3);
    data.add("foo");
    data.add("bar");
    data.add("baz");

    final BytesRefHash brh = new BytesRefHash();
    data.stream().map(BytesRef::new).forEach(brh::add);

    final BytesRefArray bra = BytesRefUtils.hashToArray(brh);

    Assert.assertEquals("Not all terms found.", data.size(), bra.size());

    final BytesRefIterator bri = bra.iterator();
    BytesRef br;
    while ((br = bri.next()) != null) {
      Assert.assertNotSame("BytesRef not found.", -1, brh.find(br));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testHashToArray_empty()
      throws Exception {
    final BytesRefHash brh = new BytesRefHash();
    final BytesRefArray bra = BytesRefUtils.hashToArray(brh);
    Assert.assertEquals("Expected an empty BytesRefArray.", 0, bra.size());
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testHashToSet()
      throws Exception {
    final Collection<String> data = new HashSet<>(3);
    data.add("foo");
    data.add("bar");
    data.add("baz");

    final BytesRefHash brh = new BytesRefHash();
    data.stream().map(BytesRef::new).forEach(brh::add);

    final Set<String> result = BytesRefUtils.hashToSet(brh);

    Assert.assertNotNull("Result was null.", result);
    Assert.assertEquals("Not all terms returned.", data.size(), result.size());

    Assert.assertEquals("Not all BytesRefs found.",
        data.size(), data.stream().filter(result::contains).count());
  }

  @Test
  public void testHashToSet_empty()
      throws Exception {
    final BytesRefHash brh = new BytesRefHash();
    final Set<String> result = BytesRefUtils.hashToSet(brh);
    Assert.assertNotNull("Result was null.", result);
    Assert.assertTrue("Expected an empty Set.", result.isEmpty());
  }
}