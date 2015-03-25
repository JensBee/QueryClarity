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

package de.unihildesheim.iw.xml;

import de.unihildesheim.iw.xml.elements.Language;
import de.unihildesheim.iw.xml.elements.Passage;
import de.unihildesheim.iw.xml.elements.PassagesGroup;
import de.unihildesheim.iw.xml.elements.ScoreType;
import org.jetbrains.annotations.NotNull;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.util.Iterator;

/**
 * @author Jens Bertram
 */
public final class TopicsXMLWriter
    extends TopicsXMLReader {
  /**
   * Marshaller for {@link #jaxbContext}.
   */
  private final Marshaller jaxbMarshaller;

  /**
   * Constructor setting the source file to read base data from.
   * @param source Source file
   * @throws JAXBException Thrown if parsing the source file failed
   */
  public TopicsXMLWriter(@NotNull final File source)
      throws JAXBException {
    super(source, false);
    this.jaxbMarshaller = getJaxbContext().createMarshaller();
    this.jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
        Boolean.TRUE);
  }

  /**
   * Set the scoring type used.
   * @param scoreType Type
   */
  public void addScoreType(@NotNull final ScoreType scoreType) {
    getTopicPassages().getScoreTypes().add(scoreType);
  }

  /**
   * Add a list of words used as stopwords.
   * @param lang Words list
   */
  public void addStopwordsList(@NotNull final Language lang) {
    getTopicPassages().getLanguages().add(lang);
  }

  /**
   * Write results to a new file.
   * @param out Target file
   * @throws JAXBException Thrown if marshalling to the target file failed
   */
  public void writeResults(@NotNull final File out)
      throws JAXBException {
    writeResults(out, false);
  }

  /**
   * Write results to a new file.
   * @param out Target file
   * @param strip If true, empty elements will not be written
   * @throws JAXBException Thrown if marshalling to the target file failed
   */
  @SuppressWarnings("BooleanParameter")
  public void writeResults(final File out, final boolean strip)
      throws JAXBException {
    if (strip) {
      final Iterator<PassagesGroup> passageGroupIt =
          getPassagesGroups().iterator();
      while (passageGroupIt.hasNext()) {
        final PassagesGroup pg = passageGroupIt.next();
        final Iterator<Passage> passageIt = pg.getPassages().iterator();
        // remove passages without scores
        while (passageIt.hasNext()) {
          final Passage p = passageIt.next();
          if (p.getScores().isEmpty()) {
            passageIt.remove();
          }
        }
        // remove passage groups without any scored passage
        if (pg.getPassages().isEmpty()) {
          passageGroupIt.remove();
        }
      }
    }
    this.jaxbMarshaller.marshal(getTopicPassages(), out);
  }
}
