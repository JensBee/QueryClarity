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

package de.unihildesheim.iw.data;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.data.IPCCode.IPCRecord;
import de.unihildesheim.iw.data.IPCCode.IPCRecord.Field;
import de.unihildesheim.iw.data.IPCCode.InvalidIPCCodeException;
import de.unihildesheim.iw.data.IPCCode.Parser;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Test for {@link IPCCode}.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
@SuppressWarnings("JavaDoc")
public class IPCCodeTest
    extends TestCase {
  public IPCCodeTest() {
    super(LoggerFactory.getLogger(IPCCodeTest.class));
  }

  private static final Character[] A_TO_Z = {'a', 'b', 'c', 'd', 'e', 'f', 'g',
      'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
      'v', 'w', 'x', 'y', 'z'};

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testParse_allValid()
      throws Exception {
    final int expectedFieldsCount = Field.values().length;
    // section (a-h)
    Arrays.stream(A_TO_Z, 0, 8)
        .parallel().forEach(sec -> {
      // class (01-99)
      IntStream.range(1, 100).forEach(cls -> {
        final String clsStr = cls < 10 ? "0" + cls : String.valueOf(cls);
        // subclass (a-z)
        Arrays.stream(A_TO_Z, 0, 26).forEach(scls -> {
          // main group (1-9999)
          IntStream.of(1, 9, 10, 11, 99, 100, 111, 999, 1000, 1111, 9999)
              .forEach(grp -> {
                // sub group (00-9999)
                IntStream.of(0, 1, 9, 10, 99, 100, 111, 999, 1000, 1111, 9999)
                    .forEach(sgrp -> {
                      final String sgrpStr =
                          sgrp < 10 ? "0" + sgrp : String.valueOf(sgrp);
                      final IPCRecord ipcRec;
                      try {
                        ipcRec = IPCCode.parse(
                            String.valueOf(sec) + clsStr + scls + grp +
                                Parser.DEFAULT_SEPARATOR + sgrpStr);
                        Assert.assertEquals(
                            "Code expected to be valid (" + ipcRec + ").",
                            ipcRec.getSetFields().size(), expectedFieldsCount);
                      } catch (final InvalidIPCCodeException e) {
                        Assert.fail("Caught exception! " + e);
                      }
                    });
              });
        });
      });
    });
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testParse_customSeparator()
      throws InvalidIPCCodeException {
    final int expectedFieldsCount = Field.values().length;
    final IPCRecord ipc = IPCCode.parse("A61K0031-4166", '-');
    Assert.assertEquals(
        "Code expected to be valid (" + ipc + ").",
        ipc.getSetFields().size(), expectedFieldsCount);
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testParse_zeroPad()
      throws InvalidIPCCodeException {
    final Parser ipcParser = new Parser();
    ipcParser.allowZeroPad(true);
    ipcParser.separatorChar('-');
    IPCRecord ipc;

    ipc = ipcParser.parse("A61K0000-0000");
    Assert.assertEquals(
        "Code expected to be valid (" + ipc + ").",
        3L, ipc.getSetFields().size());

    ipc = ipcParser.parse("A61K0-0000");
    Assert.assertEquals(
        "Code expected to be valid (" + ipc + ").",
        3L, ipc.getSetFields().size());

    ipc = ipcParser.parse("A61K-0000");
    Assert.assertEquals(
        "Code expected to be valid (" + ipc + ").",
        3L, ipc.getSetFields().size());

    ipc = ipcParser.parse("A61K");
    Assert.assertEquals(
        "Code expected to be valid (" + ipc + ").",
        3L, ipc.getSetFields().size());

    ipc = ipcParser.parse("A61K0000");
    Assert.assertEquals(
        "Code expected to be valid (" + ipc + ").",
        3L, ipc.getSetFields().size());

    ipc = ipcParser.parse("A61K00-0");
    Assert.assertEquals(
        "Code expected to be valid (" + ipc + ").",
        3L, ipc.getSetFields().size());
  }
}