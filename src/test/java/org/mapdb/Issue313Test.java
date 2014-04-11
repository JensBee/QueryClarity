/*
 * Copyright (C) 2014 Jens Bertram
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
package org.mapdb;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import static org.mapdb.DBMaker.JVMSupportsLargeMappedFiles;

/**
 *
 * @author Jens Bertram
 */
public class Issue313Test {

  @Test
  public void negativePositionTest() throws InterruptedException {
    System.out.print("JVMSupportsLargeMappedFiles: "
            + JVMSupportsLargeMappedFiles());
    DB db = DBMaker.newFileDB(new File("npTest"))
            //            .mmapFileEnableIfSupported()
            //            .mmapFileEnable()
            .mmapFileEnablePartial()
            .transactionDisable()
            .asyncWriteEnable()
            .asyncWriteFlushDelay(100)
            .make();

    Map<String, String> map = db.createTreeMap("data").make();
    for (long i = 0; i < Long.MAX_VALUE; i++) {
      map.put("Entry" + i, " some data " + UUID.randomUUID().toString());
    }
  }
}
