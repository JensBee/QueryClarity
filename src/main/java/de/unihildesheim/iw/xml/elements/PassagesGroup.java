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

import org.jetbrains.annotations.Nullable;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A passages grouping element.
 */
@XmlRootElement
public final class PassagesGroup {
  /**
   * Default number of passages that are expected. Used as list initializer.
   */
  private static final int DEFAULT_PASSAGES_SIZE = 20;

  /**
   * List of single passages in this group.
   */
  private final Collection<Passage> passages =
      new ArrayList<>(DEFAULT_PASSAGES_SIZE);

  /**
   * PassagesGroup group source attribute.
   */
  @Nullable
  private String source;

  /**
   * Default constructor used for JAXB (un)marshalling.
   */
  public PassagesGroup() {
  }

  /**
   * Create a new passages group with the given source attribute.
   *
   * @param newSource Source name
   */
  public PassagesGroup(@Nullable final String newSource) {
    this.source = newSource;
  }

  /**
   * Get the list of passages in this group.
   *
   * @return List of passages in this group
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  @XmlElement(name = "p", type = Passage.class)
  public Collection<Passage> getPassages() {
    return this.passages;
  }

  /**
   * Get the source name of this passage group.
   *
   * @return Source name
   */
  @Nullable
  @XmlAttribute
  public String getSource() {
    return this.source;
  }

  /**
   * Set the source for this passages group.
   *
   * @param src Source name
   */
  public void setSource(@Nullable final String src) {
    this.source = src;
  }
}
