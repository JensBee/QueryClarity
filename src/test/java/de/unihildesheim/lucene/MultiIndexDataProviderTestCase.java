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
package de.unihildesheim.lucene;

import de.unihildesheim.TestConfig;
import de.unihildesheim.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class MultiIndexDataProviderTestCase {

  /**
   * DataProvider instance currently in use.
   */
  private final Class<? extends IndexDataProvider> dataProvType;

  /**
   * Test documents index.
   */
  @SuppressWarnings("ProtectedField")
  protected static TestIndex index;

  protected MultiIndexDataProviderTestCase(
          final Class<? extends IndexDataProvider> dataProv) {
    this.dataProvType = dataProv;
  }

  protected static final Collection<
        Class<? extends IndexDataProvider>> getDataProvider() {
    final Collection<Class<? extends IndexDataProvider>> providers
            = new ArrayList<>(2);
    providers.add(DirectIndexDataProvider.class);
//    providers.add(CachedIndexDataProvider.class); BROKEN!
    return providers;
  }

  protected static final Collection<Object[]> getCaseParameters() {
    final Collection<Class<? extends IndexDataProvider>> providers
            = getDataProvider();
    final Collection<Object[]> params = new ArrayList<>(providers.size());

    for (Class<? extends IndexDataProvider> prov : getDataProvider()) {
      params.add(new Object[]{prov});
    }
    params.add(new Object[]{null});
    return params;
  }

  protected final void caseSetUp() throws IOException, InstantiationException,
          IllegalAccessException {
    Environment.clear();
    if (this.dataProvType == null) {
      index.setupEnvironment();
    } else {
      index.setupEnvironment(this.dataProvType);
    }
    index.clearTermData();
    Environment.clearAllProperties();
  }
}
