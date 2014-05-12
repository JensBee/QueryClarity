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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.util.RandomValue;

import java.util.Set;

/**
 * Test for {@link DirectIndexDataProvider}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings({"UnusedDeclaration", "EmptyMethod"})
public final class DirectIndexDataProviderTest
    extends IndexDataProviderTestCase {

  /**
   * Initialize the test.
   *
   * @throws Exception Any exception indicates an error
   */
  public DirectIndexDataProviderTest()
      throws Exception {
    super(new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL));
  }

  @Override
  protected Class<? extends IndexDataProvider> getInstanceClass() {
    return DirectIndexDataProvider.class;
  }

  @Override
  protected IndexDataProvider createInstance(final Set<String> fields,
      final Set<String> stopwords)
      throws Exception {
    return new DirectIndexDataProvider.Builder()
        .temporary()
        .dataPath(TestIndexDataProvider.reference.getDataDir())
        .documentFields(fields)
        .indexPath(TestIndexDataProvider.reference.getIndexDir())
        .stopwords(stopwords)
        .createCache("test-" + RandomValue.getString(16))
        .warmup()
        .build();
  }
}
