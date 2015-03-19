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

package de.unihildesheim.iw.lucene.query;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * @author Jens Bertram
 */
public interface TermsProvidingQuery {
  /**
   * Get the list of terms from the original query. Stop-words may be removed.
   *
   * @return List of query terms (with stop-words removed)
   */
  @NotNull
  Collection<String> getQueryTerms();
}
