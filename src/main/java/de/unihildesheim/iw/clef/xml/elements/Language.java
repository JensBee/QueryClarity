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

import de.unihildesheim.iw.util.StringUtils;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * @author Jens Bertram
 */
@XmlRootElement
public final class Language {

  private String stopwords = "";

  private String lang = "";

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
   * Set a list of strings as stopwords.
   *
   * @param newStopwords Stopwords
   */
  public final void setStopwords(final Collection<String> newStopwords) {
    final List<String> sw = new ArrayList<>(new HashSet<>(newStopwords));
    Collections.sort(sw);
    this.stopwords = StringUtils.join(sw, " ");
  }

  /**
   * Set a space delimited string as list of stopwords.
   *
   * @param newStopwords Stopwords as single string
   */
  public final void setStopwords(final String newStopwords) {
    final List<String> sw = new ArrayList<>(
        new HashSet<>(StringUtils.split(newStopwords, " ")));
    Collections.sort(sw);
    this.stopwords = StringUtils.join(sw, " ");
  }

  @Override
  public int hashCode() {
    int result = this.stopwords.hashCode();
    result = 31 * result + this.lang.hashCode();
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Language) {
      final Language other = (Language) obj;
      if (this.lang.equalsIgnoreCase(other.lang)) {
        final Collection<String> thisStopwords =
            new ArrayList<>(StringUtils.split(this.stopwords, " "));
        final Collection<String> thatStopwords =
            new ArrayList<>(StringUtils.split(other.stopwords, " "));
        if (thisStopwords.size() == thatStopwords.size() && thisStopwords
            .containsAll(thatStopwords)) {
          return true;
        }
      }
    }
    return false;
  }
}
