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

package de.unihildesheim.iw.lucene;

import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;

/**
 * Shared default values for classes using the {@link CommonTermsQuery} or
 * similar functions to avoid common terms.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class CommonTermsDefaults {
  /**
   * Default boolean operator for low frequent terms query.
   */
  public static final Occur LFOP_DEFAULT = Occur.SHOULD;
  /**
   * Default boolean operator for high frequent terms query.
   */
  public static final Occur HFOP_DEFAULT = Occur.SHOULD;
  /**
   * Default maximum threshold value. Maximum threshold [0..1] of a terms
   * document frequency to be considered a low frequency term.
   */
  public static final float MTF_DEFAULT = 0.01f;
}
