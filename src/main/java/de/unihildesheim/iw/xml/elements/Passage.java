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

import de.unihildesheim.iw.lucene.scoring.ScoringResult.ScoringResultXml;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Jens Bertram
 */
@XmlRootElement
public final class Passage {
  /**
   * Default number of scores that are expected. Used as list initializer.
   */
  private static final int DEFAULT_SCORES_SIZE = 10;
  /**
   * List of scores calculated for this passage.
   */
  private final Collection<Score> scores = new ArrayList<>(DEFAULT_SCORES_SIZE);
  /**
   * Passage content.
   */
  @Nullable
  private String content;
  /**
   * Passages language attribute.
   */
  @Nullable
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
  public Passage(
      @Nullable final String newLang,
      @Nullable final String newContent) {
    this.lang = newLang;
    this.content = newContent;
  }

  /**
   * Get the language of this passage
   *
   * @return Language identifier
   */
  @XmlAttribute(name = "lang")
  @Nullable
  public String getLanguage() {
    return this.lang;
  }

  /**
   * Set the language attribute for this passage.
   *
   * @param newLang Language identifier
   */
  public void setLanguage(@NotNull final String newLang) {
    this.lang = newLang;
  }

  /**
   * Get the content of this passage.
   *
   * @return Passage content
   */
  @Nullable
  public String getContent() {
    return this.content;
  }

  /**
   * Set the content for this passage.
   *
   * @param newContent Content
   */
  public void setContent(@Nullable final String newContent) {
    this.content = newContent;
  }

  /**
   * Get the list of calculated scores.
   *
   * @return List of calculated scores
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  @XmlElement(name = "score", type = Score.class)
  public Collection<Score> getScores() {
    return this.scores;
  }

  /**
   * Scoring result XML element.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Score {
    /**
     * Implementation name.
     */
    @Nullable
    @XmlAttribute
    private String impl;
    /**
     * Flag indicating, if this result is empty.
     */
    @XmlAttribute
    private boolean empty;
    /**
     * Score result.
     */
    @Nullable
    @XmlAttribute
    private Double score;
    /**
     * Complete result set.
     */
    @Nullable
    private ScoringResultXml result;

    /**
     * Default constructor used for JAXB (un)marshalling.
     */
    public Score() {
    }

    /**
     * Create a new score result.
     *
     * @param identifier Name identifying the implementation type
     * @param newScore Result score
     * @param isEmpty True, if result is empty
     */
    @SuppressWarnings("BooleanParameter")
    public Score(
        @Nullable final String identifier,
        @Nullable final Double newScore, final boolean isEmpty) {
      this.impl = identifier;
      this.score = newScore;
      this.empty = isEmpty;
    }

    /**
     * Check, if there's no content for this passage.
     * @return True, if no content
     */
    public boolean isEmpty() {
      return this.empty;
    }

    /**
     * Get the identifier describing this passage.
     * @return Identifier
     */
    @Nullable
    public String getIdentifier() {
      return this.impl;
    }

    /**
     * Get the score calculated for this passage.
     * @return Score value
     */
    @Nullable
    public Double getScore() {
      return this.score;
    }

    /**
     * Get the result element.
     *
     * @return Scoring result
     */
    @Nullable
    @XmlElement(name = "result")
    public ScoringResultXml getResult() {
      return this.result;
    }

    /**
     * Sets the scoring result.
     *
     * @param newResult Scoring result
     */
    public void setResult(
        @Nullable final ScoringResultXml newResult) {
      this.result = newResult;
    }
  }
}
