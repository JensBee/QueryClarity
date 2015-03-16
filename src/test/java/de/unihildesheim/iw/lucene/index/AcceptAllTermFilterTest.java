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

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.index.TermFilter.AcceptAll;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link TermFilter.AcceptAll}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class AcceptAllTermFilterTest
    extends TestCase {
  public AcceptAllTermFilterTest() {
    super(LoggerFactory.getLogger(AcceptAllTermFilterTest.class));
  }

  @Test
  public void testIsAccepted()
      throws Exception {
    final AcceptAll aaTf = new AcceptAll();

    Assert.assertTrue("Term not accepted.",
        aaTf.isAccepted(null, new BytesRef("foo")));
    Assert.assertTrue("Term not accepted.",
        aaTf.isAccepted(null, new BytesRef("bar")));
    Assert.assertTrue("Term not accepted.",
        aaTf.isAccepted(null, new BytesRef("1")));
    Assert.assertTrue("Term not accepted.",
        aaTf.isAccepted(null, new BytesRef("")));
  }
}