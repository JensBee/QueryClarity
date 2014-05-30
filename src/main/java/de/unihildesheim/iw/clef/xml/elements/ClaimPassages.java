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

package de.unihildesheim.iw.clef.xml.elements;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * JAXB for {@code claimpassages} XML documents.
 *
 * @author Jens Bertram
 */
@XmlRootElement
public class ClaimPassages {

  /**
   * Default number of passages that are expected. Used as list initializer.
   */
  private static final int DEFAULT_PASSAGES_SIZE = 100;
  /**
   * Passage elements list.
   */
  private Collection<PassagesGroup> passagesGroups =
      new ArrayList<>(DEFAULT_PASSAGES_SIZE);
  /**
   * Default number of score types that are expected. Used as list initializer.
   */
  private static final int DEFAULT_SCORETYPES_SIZE = 10;
  /**
   * Passage elements list.
   */
  private Collection<ScoreType> scoreTypes =
      new ArrayList<>(DEFAULT_SCORETYPES_SIZE);
  /**
   * Default number of languages that are expected. Used as list initializer.
   */
  private static final int DEFAULT_LANGUAGE_SIZE = 3;
  /**
   * Language elements list.
   */
  private Collection<Language> languages =
      new ArrayList<>(DEFAULT_LANGUAGE_SIZE);

  /**
   * Default constructor used for JAXB (un)marshalling.
   */
  public ClaimPassages() {
  }

  /**
   * Get the list of passages groups.
   *
   * @return List of passages groups
   */
  @XmlElementWrapper(name = "passages")
  @XmlElement(name = "passages", type = PassagesGroup.class)
  public Collection<PassagesGroup> getPassageGroups() {
    return this.passagesGroups;
  }

  /**
   * Set the passages group list.
   *
   * @param groupList Groups to add
   */
  public void setPassageGroups(final List<PassagesGroup> groupList) {
    this.passagesGroups = groupList;
  }

  @XmlElementWrapper(name = "languages")
  @XmlElement(name = "lang", type = Language.class)
  public Collection<Language> getLanguages() {
    return this.languages;
  }

  /**
   * Get the list of score types.
   *
   * @return List of score types
   */
  @XmlElement(name = "scores", type = ScoreType.class)
  public Collection<ScoreType> getScoreTypes() {
    return this.scoreTypes;
  }
}
