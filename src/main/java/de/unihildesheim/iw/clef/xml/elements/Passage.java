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

import de.unihildesheim.iw.lucene.scoring.ScoringResult;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Jens Bertram
 */
@XmlRootElement
public class Passage {
  /**
   * Default number of scores that are expected. Used as list initializer.
   */
  private static final int DEFAULT_SCORES_SIZE = 10;
  private final Collection<Score> scores = new ArrayList<>(DEFAULT_SCORES_SIZE);
  /**
   * Passage content.
   */
  private String content;
  /**
   * Passages language attribute.
   */
  private String lang;

  /**
   * Default constructor used for JAXB (un)marshalling.
   */
  public Passage() {
  }

  /**
   * Create a new passage with the given language attribute set.
   *
   * @param newLang Language identifier
   * @param newContent Passage content
   */
  public Passage(final String newLang, final String newContent) {
    this.lang = newLang;
    this.content = newContent;
  }

  /**
   * Get the language of this passage
   *
   * @return Language identifier
   */
  @XmlAttribute(name = "lang")
  public String getLanguage() {
    return this.lang;
  }

  /**
   * Set the language attribute for this passage.
   *
   * @param newLang Language identifier
   */
  public void setLanguage(final String newLang) {
    this.lang = newLang;
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

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  @XmlElement(name = "score", type = Score.class)
  public Collection<Score> getScores() {
    return this.scores;
  }

  public static class Score {
    @XmlAttribute
    private final String impl;

    @XmlAttribute
    private final boolean empty;

    @XmlAttribute
    private final Double score;

    private ScoringResult.ScoringResultXml result;

    public Score(final String identifier, final Double newScore,
        final boolean isEmpty) {
      this.impl = identifier;
      this.score = newScore;
      this.empty = isEmpty;
    }

    @XmlElement
    public ScoringResult.ScoringResultXml getResult() {
      return this.result;
    }

    public void setResult(final ScoringResult.ScoringResultXml newResult) {
      this.result = newResult;
    }
  }
}
