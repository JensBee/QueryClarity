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

package de.unihildesheim.iw.lucene.analyzer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.fr.FrenchLightStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * @author Jens Bertram
 */
public final class FrenchAnalyzer
    extends StopwordAnalyzerBase {
  /**
   * Default elisions to exclude.
   */
  static final String[] DEFAULT_ELISIONS = {
      "c", "d", "j", "l", "m", "n", "qu", "s", "t",
      "jusqu", "quoiqu", "lorsqu", "puisqu"};
  /**
   * Array view of elisions to remove. May be extended by user.
   */
  private final CharArraySet elisions =
      new CharArraySet(DEFAULT_ELISIONS.length, true);

  /**
   * Builds an analyzer without any stop words.
   */
  public FrenchAnalyzer() {
    this.elisions.addAll(Arrays.asList(DEFAULT_ELISIONS));
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param newStopwords stop words
   */
  public FrenchAnalyzer(@Nullable final CharArraySet newStopwords) {
    super(newStopwords);
    this.elisions.addAll(Arrays.asList(DEFAULT_ELISIONS));
  }

  /**
   * This configuration must match with the configuration used for the index!
   *
   * @param fieldName Document field
   * @return Token stream
   */
  @SuppressWarnings("resource")
  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    TokenStream result = new StandardFilter(source);
    result = new ElisionFilter(result, this.elisions);
    result = new LowerCaseFilter(result);
    result = new StopFilter(result, getStopwordSet());
    result = new FrenchLightStemFilter(result);
    return new TokenStreamComponents(source, result);
  }
}
