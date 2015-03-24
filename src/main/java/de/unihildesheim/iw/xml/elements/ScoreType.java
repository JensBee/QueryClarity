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

package de.unihildesheim.iw.xml.elements;

import de.unihildesheim.iw.xml.adapters.MapAdapter.StringValue;
import org.jetbrains.annotations.Nullable;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Map;

/**
 * Score type XML element.
 *
 * @author Jens Bertram
 */
@XmlRootElement
public final class ScoreType {
  /**
   * Score type identifier.
   */
  @Nullable
  private String identifier;

  /**
   * Configuration.
   */
  @Nullable
  private Map<String, String> confMap;

  /**
   * Get the configuration map.
   *
   * @return Configuration map
   */
  @Nullable
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  @XmlElement(name = "conf")
  @XmlJavaTypeAdapter(StringValue.class)
  public Map<String, String> getConfiguration() {
    return this.confMap;
  }

  /**
   * Set the configuration parameters map.
   *
   * @param map Configuration map
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  public void setConfiguration(@Nullable final Map<String, String> map) {
    this.confMap = map;
  }

  /**
   * Get the score type of this Score
   *
   * @return Language identifier
   */
  @Nullable
  @XmlAttribute(name = "impl")
  public String getImplementation() {
    return this.identifier;
  }

  /**
   * Set the score type attribute for this Score.
   *
   * @param id Score identifier
   */
  public void setImplementation(@Nullable final String id) {
    this.identifier = id;
  }
}
