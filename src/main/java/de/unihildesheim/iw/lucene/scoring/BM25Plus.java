/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

package de.unihildesheim.iw.lucene.scoring;

import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.BigMathFunctions;
import de.unihildesheim.iw.util.GlobalConfiguration;
import de.unihildesheim.iw.util.concurrent.AtomicBigDecimal;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class BM25Plus {

  /**
   * Default math context for model calculations.
   */
  @SuppressWarnings("WeakerAccess")
  static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf()
          .getString(GlobalConfiguration.DefaultKeys.MATH_CONTEXT.toString(),
              GlobalConfiguration.DEFAULT_MATH_CONTEXT));
  private static final BigDecimal ONE_HALF = BigDecimal.valueOf(0.5);

  private final IndexDataProvider dataProv;

  private final BigDecimal idxSize;
  private final BigDecimal avgDocLength;

  private final BigDecimal kVal;
  private final BigDecimal kPlusOne;

  private final BigDecimal bVal;
  private final BigDecimal oneSubB;

  private final BigDecimal deltaVal;

  private static final BigDecimal LN10 = BigMathFunctions.ln(BigDecimal
      .valueOf(10.0), MATH_CONTEXT.getPrecision());

  /**
   * Free parameter K.
   */
  private double k = 2.0;
  /**
   * Free parameter B.
   */
  private double b = 0.75;
  /**
   * Free parameter delta.
   */
  private double delta = 1.0;

  public BM25Plus(@NotNull final IndexDataProvider idxDataProv) {
    this.dataProv = idxDataProv;
    this.idxSize = BigDecimal.valueOf(this.dataProv.getDocumentCount());
    this.avgDocLength = calcAvgDl();

    this.kVal = BigDecimal.valueOf(this.k);
    this.kPlusOne = this.kVal.add(BigDecimal.ONE, MATH_CONTEXT);

    this.bVal = BigDecimal.valueOf(this.b);
    this.oneSubB = BigDecimal.ONE.subtract(this.bVal, MATH_CONTEXT);

    this.deltaVal = BigDecimal.valueOf(this.delta);
  }

  public BigDecimal score(final int docId,
      @NotNull final BytesRefArray queryTerms) {
    final DocumentModel dm = this.dataProv.getDocumentModel(docId);
    final AtomicBigDecimal score = new AtomicBigDecimal(BigDecimal.ZERO);

//    StreamUtils.stream(queryTerms).forEach(qt -> {
//      final BigDecimal normalizedDf = BigDecimal.valueOf(dm.tf(qt))
//          .divide(this.oneSubB.add(
//              this.bVal.multiply(BigDecimal.valueOf(dm.tf())
//                  .divide(this.avgDocLength, MATH_CONTEXT), MATH_CONTEXT),
//              MATH_CONTEXT), MATH_CONTEXT);
//      score.add(calcIdf(qt).multiply(
//          this.kPlusOne.multiply(normalizedDf, MATH_CONTEXT)
//              .divide(
//                  this.kVal.add(normalizedDf, MATH_CONTEXT), MATH_CONTEXT
//              ), MATH_CONTEXT), MATH_CONTEXT);
//    });

    StreamUtils.stream(queryTerms).forEach(qt -> {
      final BigDecimal inDocFreq = BigDecimal.valueOf(dm.tf(qt));
      final BigDecimal docLength = BigDecimal.valueOf(dm.tf());
      final BigDecimal dividend =
          inDocFreq.multiply(this.kPlusOne, MATH_CONTEXT);
      final BigDecimal divisor = inDocFreq.add(
          this.kVal.multiply(this.oneSubB.add(this.bVal.multiply(
              docLength.divide(this.avgDocLength, MATH_CONTEXT)
              , MATH_CONTEXT), MATH_CONTEXT), MATH_CONTEXT)
          , MATH_CONTEXT);
      score.add(
          calcIdf(qt).multiply(
              dividend.divide(divisor, MATH_CONTEXT),
              MATH_CONTEXT).add(this.deltaVal), MATH_CONTEXT);
    });
    return score.get();
  }

  private BigDecimal calcIdf(@NotNull final BytesRef term) {
//    final BigDecimal docFreq = BigDecimal.valueOf(
//        (long) this.dataProv.getDocumentFrequency(term));


//    final BigDecimal termDf = BigDecimal.valueOf((long) this.dataProv
//        .getDocumentFrequency(term));
//    return BigMathFunctions.ln(
//        this.idxSize.add(BigDecimal.ONE, MATH_CONTEXT)
//            .divide(termDf.add(ONE_HALF), MATH_CONTEXT),
//        MATH_CONTEXT.getPrecision());

    return BigMathFunctions.ln(
        this.idxSize.add(BigDecimal.ONE, MATH_CONTEXT)
            .divide(BigDecimal.valueOf((long) this.dataProv
                .getDocumentFrequency(term)), MATH_CONTEXT),
        MATH_CONTEXT.getPrecision())
        .divide(LN10, MATH_CONTEXT);

//    final BigDecimal dividend =
//        this.idxSize.subtract(docFreq, MATH_CONTEXT).add(ONE_HALF);
//    final BigDecimal divisor = docFreq.add(ONE_HALF, MATH_CONTEXT);
//    return BigMathFunctions.ln(dividend.divide(divisor, MATH_CONTEXT),
//        MATH_CONTEXT.getPrecision());
  }

  /**
   * Calculate the average document length.
   */
  private BigDecimal calcAvgDl() {
    return BigDecimal
        .valueOf(this.dataProv.getTermFrequency())
        .divide(
            BigDecimal.valueOf(this.dataProv.getDocumentCount()), MATH_CONTEXT);
  }
}
