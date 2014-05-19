/*
 * Copyright (C) 2014 Jens Bertram
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

package de.unihildesheim.iw.lucene.util.concurrent.processing;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jens Bertram
 */
public class TermsEnumSource
    extends Source<Tuple.Tuple2<ByteArray, Long>> {
  /**
   * {@link TermsEnum} to use for collecting terms.
   */
  private final TermsEnum tEnum;

  /**
   * Number of provided items.
   */
  private final AtomicLong sourcedItemCount;

  public TermsEnumSource(final TermsEnum te) {
    super();
    this.tEnum = Objects.requireNonNull(te);
    this.sourcedItemCount = new AtomicLong(0);
  }

  @Override
  public long getSourcedItemCount() {
    return this.sourcedItemCount.get();
  }

  @Override
  public synchronized Tuple.Tuple2<ByteArray, Long> next()
      throws ProcessingException, InterruptedException {
    if (isFinished()) {
      throw new ProcessingException.SourceHasFinishedException();
    }
    try {
      BytesRef br;
      br = this.tEnum.next();
      if (br == null) {
        stop();
        return null;
      }
      // Try to seek to term. If not found, try next term.
      while (!this.tEnum.seekExact(br)) {
        br = this.tEnum.next();
        if (br == null) {
          stop();
          return null;
        }
      }
      this.sourcedItemCount.incrementAndGet();
      final Tuple.Tuple2<ByteArray, Long> ret = Tuple
          .tuple2(BytesRefUtils.toByteArray(br), this.tEnum.totalTermFreq());
      return ret;
    } catch (IOException e) {
      throw new ProcessingException.SourceFailedException(e);
    }
  }

  @Override
  public Long getItemCount()
      throws ProcessingException {
    return null;
  }
}
