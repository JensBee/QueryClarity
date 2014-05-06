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


package de.unihildesheim.iw.lucene.document;

/**
 * Wrapper for {@link Exception}s thrown while working with document models.
 */
public class DocumentModelException
    extends Exception {

  /**
   * Serialization id.
   */
  private static final long serialVersionUID = -3614607208971266570L;

  /**
   * Create a new generic {@link DocumentModelException} to indicate that model
   * creation has failed.
   *
   * @param exception Originating exception
   */
  public DocumentModelException(
      final ReflectiveOperationException exception) {
    super("Error instantiating requested document model type.",
        exception.getCause());
  }
}
