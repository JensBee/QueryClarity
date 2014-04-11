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
import org.junit.Test;

public class Issue312Test {

  @Test
  public void test() {
    DB db = DBMaker.newFileDB(new File("rotest"))
            .mmapFileEnableIfSupported()
            .transactionDisable()
            .make();

    Map<Long, String> map = db.createTreeMap("data").make();
    for (long i = 0; i < 100000; i++) {
      map.put(i, i + "hi my friend " + i);
    }
    db.commit();
    db.close();

    db = DBMaker.newFileDB(new File("rotest"))
            .mmapFileEnableIfSupported()
            .transactionDisable()
            .readOnly()
            .make();

  }
}
