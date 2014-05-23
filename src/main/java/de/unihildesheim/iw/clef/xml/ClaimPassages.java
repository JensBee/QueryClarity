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

package de.unihildesheim.iw.clef.xml;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;
import java.util.List;

/**
 * JAXB for claimpassages XML documents.
 *
 * @author Jens Bertram
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "claimpassages")
public class ClaimPassages {

  /**
   * Passage elements list.
   */
  @XmlElement(name = "passages")
  private List<PassagesGroup> passagesGroups;

  /**
   * Get the list of passages groups.
   *
   * @return List of passages groups
   */
  public List<PassagesGroup> getPassagesGroups() {
    return this.passagesGroups;
  }

  /**
   * Set the passages group list.
   *
   * @param groupList Groups to add
   */
  public void setPassagesGroups(final List<PassagesGroup> groupList) {
    this.passagesGroups = groupList;
  }

  /**
   * A passages grouping element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class PassagesGroup {
    /**
     * List of single passages in this group.
     */
    @XmlElement(name = "p")
    private List<Passage> passages;

    /**
     * PassagesGroup group source attribute.
     */
    @XmlAttribute(name = "source")
    private String sourceAttr;

    /**
     * Create a new empty passages group.
     */
    public PassagesGroup() {
    }

    /**
     * Create a new passages group with the given source attribute.
     *
     * @param source Source name
     */
    public PassagesGroup(final String source) {
      this.sourceAttr = source;
    }

    /**
     * Get the list of passages in this group.
     *
     * @return List of passages in this group
     */
    public List<Passage> getPassages() {
      return this.passages;
    }

    /**
     * Add a passage to this passages group.
     *
     * @param passageList Passages to add
     */
    public void setPassages(final List<Passage> passageList) {
      this.passages = passageList;
    }

    /**
     * Get the source name of this passage group.
     *
     * @return Source name
     */
    public String getSource() {
      return this.sourceAttr;
    }

    /**
     * Set the source for this passages group.
     *
     * @param src Source name
     */
    public void setSource(final String src) {
      this.sourceAttr = src;
    }
  }

  /**
   * A single passage element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class Passage {
    /**
     * Passage content.
     */
    @XmlValue
    private String content;

    /**
     * Passages language attribute.
     */
    @XmlAttribute(name = "lang")
    private String language;

    /**
     * Create a new empty passage.
     */
    public Passage() {
    }

    /**
     * Create a new passage with the given language attribute set.
     *
     * @param lang Language
     */
    public Passage(final String lang) {
      this.language = lang;
    }

    /**
     * Create a new passage with the given language attribute set.
     *
     * @param lang Language identifier
     * @param newContent Passage content
     */
    public Passage(final String lang, final String newContent) {
      this.language = lang;
      this.content = newContent;
    }

    /**
     * Get the language of this passage
     *
     * @return Language identifier
     */
    public String getLanguage() {
      return this.language;
    }

    /**
     * Set the langugae attribute for this passage.
     *
     * @param lang Language identifier
     */
    public void setLanguage(final String lang) {
      this.language = lang;
    }

    /**
     * Get the content of this passage.
     *
     * @return Passage content
     */
    public String getContent() {
      return this.content;
    }

    /**
     * Set the content for this passage.
     *
     * @param newContent Content
     */
    public void setContent(final String newContent) {
      this.content = newContent;
    }
  }
}
