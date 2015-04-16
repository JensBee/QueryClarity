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
package de.unihildesheim.iw.lucene.scoring;

import de.unihildesheim.iw.Tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Wrapper to store scoring results.
 *
 * @author Jens Bertram
 */
public abstract class ScoringResult {
  /**
   * Calculated result score.
   */
  private Double score = 0d;

  /**
   * Get the calculated score.
   *
   * @return Calculated score
   */
  public final double getScore() {
    return this.score;
  }

  /**
   * Set the value of the calculated score.
   *
   * @param newScore new score value
   */
  protected final void _setScore(final double newScore) {
    this.score = newScore;
  }

  /**
   * Get the type of calculation that created this result.
   *
   * @return Class creating a scoring result
   */
  public abstract Class getType();

  /**
   * Return some or all calculation related information to include in result XML
   * documents.
   *
   * @return XML object
   */
  public abstract ScoringResultXml getXml();

  /**
   * Object wrapping result information for including in XML.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ScoringResultXml {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(ScoringResultXml.class);
    /**
     * XML element key names.
     */
    public enum Keys {
      /**
       * Number of feedback documents an list of feedback documents.
       */
      FEEDBACK_DOCUMENTS("feedbackDocuments"),
      /**
       * Key for listing feedback documents.
       */
      FEEDBACK_DOCUMENT_KEY("id");

      /**
       * Key name as written in XML result.
       */
      final String name;

      /**
       * Constructor setting XML string.
       * @param newName XML string set by enum
       */
      Keys(final String newName) {
        this.name = newName;
      }

      public String toString() {
        return this.name;
      }
    }

    /**
     * List of simple String items. Mapped by key to value.
     *
     * @return List of key, value pairs
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public Map<String, String> getItems() {
      LOG.warn("XML items currently not supported!");
      return Collections.emptyMap();
    }

    /**
     * Map of list type (key, value) items, grouped by a global key.
     *
     * @return Map of key, value pairs grouped by key
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public Map<String, List<Tuple2<String, String>>> getLists() {
      //return this.lists;
      LOG.warn("XML list currently not supported!");
      return Collections.emptyMap();
    }
  }
}
