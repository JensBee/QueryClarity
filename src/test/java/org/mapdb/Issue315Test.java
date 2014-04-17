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

import org.junit.Test;

/**
 * @author Jens Bertram
 */
public class Issue315Test {

  @Test
  public void test() {
    DB db = DBMaker.newMemoryDB().make();

    final String item1 = "ITEM_ONE";
    final String item2 = "ITEM_ONE_TWO";

    db.createTreeMap(item1).make();
    db.createTreeSet(item2).make();

    System.out.println("DB: " + db.getAll().size());
    for (String dbItem : db.getAll().keySet()) {
      System.out.println("DB: " + dbItem);
    }

    db.delete(item1);

    System.out.println("DB: " + db.getAll().size());
    for (String dbItem : db.getAll().keySet()) {
      System.out.println("DB: " + dbItem);
    }
  }
}
