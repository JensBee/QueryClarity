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
package de.unihildesheim;

import de.unihildesheim.lucene.index.CachedIndexDataProvider;
import de.unihildesheim.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.lucene.index.IndexDataProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class TestConfig {

  public static final double DOUBLE_ALLOWED_DELTA = 0.0000000001;

  /**
   * Private empty constructor for utility class.
   */
  private TestConfig() {
    // empty
  }

  public static Collection<Class<? extends IndexDataProvider>> getDataProvider() {
    final Collection<Class<? extends IndexDataProvider>> providers
            = new ArrayList<>(2);
    providers.add(DirectIndexDataProvider.class);
//    providers.add(CachedIndexDataProvider.class);
    return providers;
  }

  public static Collection<Object[]> getDataProviderParameter() {
    final Collection<Class<? extends IndexDataProvider>> providers
            = getDataProvider();
    final Collection<Object[]> params = new ArrayList(providers.size());

    for (Class<? extends IndexDataProvider>  prov : getDataProvider()) {
      params.add(new Object[]{prov});
    }
    return params;
  }
}
