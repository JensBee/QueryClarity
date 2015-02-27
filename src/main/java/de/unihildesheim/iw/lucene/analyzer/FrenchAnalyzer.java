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

import de.unihildesheim.iw.lucene.index.IndexDataProvider;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author Jens Bertram
 */
public final class FrenchAnalyzer
    extends StopwordAnalyzerBase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(FrenchAnalyzer.class);
  private static final String[] DEFAULT_ELISIONS = {
      "c", "d", "j", "l", "m", "n", "qu", "s", "t",
      "jusqu", "quoiqu", "lorsqu", "puisqu"};
  private final CharArraySet elisions =
      new CharArraySet(DEFAULT_ELISIONS.length, true);

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param newStopwords stop words
   */
  public FrenchAnalyzer(final CharArraySet newStopwords) {
    super(newStopwords);
    this.elisions.addAll(Arrays.asList(DEFAULT_ELISIONS));
  }

  /**
   * Builds an analyzer with the default Lucene version and stopwords from the
   * given {@link IndexDataProvider}.
   */
  public FrenchAnalyzer(final IndexDataProvider dataProv) {
    super(new CharArraySet(dataProv.getStopwords(), true));
    this.elisions.addAll(Arrays.asList(DEFAULT_ELISIONS));
    LOG.debug("Stopwords: {}", dataProv.getStopwords());
  }

  /**
   * This configuration must match with the configuration used for the index!
   *
   * @param fieldName Document field
   * @return Token stream
   */
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
