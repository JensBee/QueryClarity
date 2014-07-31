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

package de.unihildesheim.iw.lucene.index;

import org.apache.lucene.index.IndexReader;

import java.util.Set;

public class DirectAccessIndexDataProviderTest
    extends IndexDataProviderTestCase {

  /**
   * Private empty constructor for utility class.
   */
  public DirectAccessIndexDataProviderTest() {
    super(new TestIndexDataProvider());
  }

  @Override
  protected Class<? extends IndexDataProvider> getInstanceClass() {
    return DirectAccessIndexDataProvider.class;
  }

  @Override
  protected IndexDataProvider createInstance(final String dataDir,
      final IndexReader reader, final Set<String> fields,
      final Set<String> stopwords)
      throws Exception {
    return new DirectAccessIndexDataProvider.Builder()
        .documentFields(fields)
        .indexReader(reader)
        .stopwords(stopwords)
        .build();
  }
}