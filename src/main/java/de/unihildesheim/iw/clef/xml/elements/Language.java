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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author Jens Bertram
 */
@XmlRootElement
public final class Language {

  private String stopwords;

  private String lang;

  public String getLanguage() {
    return this.lang;
  }

  @XmlAttribute(name = "name")
  public final void setLanguage(final String language) {
    this.lang = language;
  }

  public String getStopwords() {
    return this.stopwords;
  }

  /**
   * Set a space delimited string as list of stopwords.
   *
   * @param newStopwords Stopwords as single string
   */
  public final void setStopwords(final String newStopwords) {
    this.stopwords = newStopwords;
  }
}
