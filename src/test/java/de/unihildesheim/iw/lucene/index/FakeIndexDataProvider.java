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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.util.concurrent.processing.Source;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * No-Operation data-provider for testing purposes.
 */
public final class FakeIndexDataProvider
    implements IndexDataProvider {

  @Override
  public long getTermFrequency() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void warmUp() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Long getTermFrequency(final ByteArray term) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public double getRelativeTermFrequency(final ByteArray term) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void dispose() {
    // NOP
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Source<ByteArray> getTermsSource() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterator<Integer> getDocumentIdIterator() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Source<Integer> getDocumentIdSource() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getUniqueTermsCount() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Collection<ByteArray> getDocumentsTermSet(
      final Collection<Integer> docIds) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getDocumentCount() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean documentContains(final int documentId, final ByteArray term) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Long getLastIndexCommitGeneration() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<String> getDocumentFields() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<String> getStopwords() {
    return Collections.<String>emptySet();
  }

  @Override
  public boolean isDisposed() {
    return false;
  }
}
