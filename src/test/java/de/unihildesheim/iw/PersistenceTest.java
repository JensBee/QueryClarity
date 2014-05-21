/*
 * Copyright (C) 2014 bhoerdzn
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

package de.unihildesheim.iw;

import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Test for {@link Persistence}.
 *
 * @author Jens Bertram
 */
public class PersistenceTest
    extends TestCase {

  /**
   * Create a temporary directory. Will be deleted after tests have finished.
   */
  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  /**
   * Test for {@link Persistence#tryCreateDataPath(String)}.
   * <p/>
   * Check illegal arguments for target path.
   *
   * @throws Exception
   */
  @Test
  public void testTryCreateDataPath_fail()
      throws Exception {
    final Collection<String> illegalArguments = new ArrayList<>();
    illegalArguments.add("");
    illegalArguments.add("  ");

    for (String arg : illegalArguments) {
      try {
        Persistence.tryCreateDataPath(arg);
        Assert.fail("Expected an Exception to be thrown.");
      } catch (IllegalArgumentException e) {
        // pass
      }
    }

    try {
      Persistence.tryCreateDataPath(null);
      Assert.fail("Expected an Exception to be thrown.");
    } catch (NullPointerException e) {
      // pass
    }
  }

  /**
   * Test for {@link Persistence#tryCreateDataPath(String)}.
   * <p/>
   * Target directory already exists as a file with same name.
   *
   * @throws Exception
   */
  @Test
  public void testTryCreateDataPath_existsAsFile()
      throws Exception {
    final String fName = "testCreateExistAsFile";
    tmpDir.newFile(fName);
    final String path = FileUtils.makePath(tmpDir.getRoot().getPath()) + fName;
    try {
      Persistence.tryCreateDataPath(path);
      Assert.fail("Expected an Exception to be thrown.");
    } catch (IOException e) {
      // pass
    }
  }

  /**
   * Test for {@link Persistence#tryCreateDataPath(String)}.
   * <p/>
   * Test create single directory.
   *
   * @throws Exception
   */
  @Test
  public void testTryCreateDataPath_single()
      throws Exception {
    final String path = FileUtils.makePath(tmpDir.getRoot().getPath()) +
        "testCreate";
    Persistence.tryCreateDataPath(path);
  }

  /**
   * Test for {@link Persistence#tryCreateDataPath(String)}.
   * <p/>
   * Test create multiple subdirectories at once.
   *
   * @throws Exception
   */
  @Test
  public void testTryCreateDataPath_multiple()
      throws Exception {
    final String path = FileUtils.makePath(tmpDir.getRoot().getPath()) +
        "testCreate0" + File.separatorChar + "testCreate1" + File
        .separatorChar + "testCreate2";
    Persistence.tryCreateDataPath(path);
  }

  @Test
  public void testBuild()
      throws Exception {
    final String dataPath = FileUtils.makePath(tmpDir.getRoot().getPath()) +
        "data";

    final Persistence.Builder pb = new Persistence.Builder();
    pb.dataPath(dataPath);
    pb.name(RandomValue.getString(1, 10));

    pb.build();
  }
}