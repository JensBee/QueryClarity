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

import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.xml.adapters.MapAdapter;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.HashMap;
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
   * Empty result. May be returned, if calculation has failed.
   */
  public static final ScoringResult EMPTY_RESULT = new ScoringResult() {
    private final ScoringResultXml xml = new ScoringResultXml();

    @Override
    public Class getType() {
      return this.getClass();
    }

    @Override
    public ScoringResultXml getXml() {
      return xml;
    }
  };

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
  @SuppressWarnings("checkstyle:methodname")
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
  public static final class ScoringResultXml {
    private final Map<String, String> items = new HashMap<>();

    private final Map<String, List<Tuple.Tuple2<String, String>>> lists = new
        HashMap<>();

    /**
     * List of simple String items. Mapped by key to value.
     *
     * @return List of key, value pairs
     */
    @XmlElement
    @XmlJavaTypeAdapter(MapAdapter.StringValue.class)
    public Map<String, String> getItems() {
      return this.items;
    }

    /**
     * Map of list type (key, value) items, grouped by a global key.
     *
     * @return Map of key, value pairs grouped by key
     */
    @XmlElement
    @XmlJavaTypeAdapter(MapAdapter.Tuple2ListValue.class)
    public Map<String, List<Tuple.Tuple2<String, String>>> getLists() {
      return this.lists;
    }
  }


}
