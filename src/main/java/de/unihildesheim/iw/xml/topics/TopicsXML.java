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

package de.unihildesheim.iw.xml.topics;

import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.xml.elements.Passage;
import de.unihildesheim.iw.xml.topics.PassagesList.Passages;
import de.unihildesheim.iw.xml.topics.Scorers.Scorer;
import org.jetbrains.annotations.NotNull;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class TopicsXML {
  /**
   * XML context.
   */
  private final JAXBContext jaxbContext;
  /**
   * XML root element, JAXB object.
   */
  private final TopicPassages topicPassages;

  /**
   * Creates a new instance from the given file.
   *
   * @param source Topics XML file. If file exists it will be loaded.
   * @throws JAXBException Thrown on unmarshalling errors
   */
  public TopicsXML(@NotNull final File source)
      throws JAXBException {
    this.jaxbContext = JAXBContext
        .newInstance(de.unihildesheim.iw.xml.topics.TopicPassages.class);
    final Unmarshaller jaxbUnmarshaller = this.jaxbContext.createUnmarshaller();

    if (source.exists()) {
      this.topicPassages = (TopicPassages) jaxbUnmarshaller.unmarshal(source);
    } else {
      this.topicPassages = new TopicPassages();
    }
  }

//  /**
//   * Extract all languages available in the topicsXML file by parsing each
//   * passage entry available.
//   *
//   * @return Set of languages
//   */
//  private Set<String> extractLanguages() {
//    return this.topicPassages.getPassagesList().getPassages().stream()
//        .flatMap(pg -> pg.getP().stream())
//        .map(p -> StringUtils.lowerCase(p.getLang()))
//        .collect(Collectors.toSet());
//  }

  /**
   * Add a scorer.
   * @param csc Clarity scorer
   * @param conf Scorer configuration
   */
  public final void addScorer(
      @NotNull final ClarityScoreCalculation csc,
      @NotNull final Configuration conf) {
    final Scorer s = new Scorer();
    s.setImpl(csc.getIdentifier());

    Scorers scorers = this.topicPassages.getScorers();
    if (scorers == null) {
      this.topicPassages.setScorers(new Scorers());
      scorers = this.topicPassages.getScorers();
    }
    scorers.getScorer().add(s);
  }

  /**
   * Get the {@link Passage Passages} by language.
   *
   * @param lang Language
   * @return List of passages
   */
  public final List<PassagesListEntry> getPassages(
      @NotNull final String lang) {
    return this.topicPassages.getPassagesList().getPassages().stream()
        .flatMap(pg -> pg.getP().stream())
        .filter(p -> p.getLang().equalsIgnoreCase(lang))
        .collect(Collectors.toList());
  }

  /**
   * Write the current XML to a file.
   *
   * @param out Target file
   * @throws JAXBException Thrown if marshalling to the target file failed
   */
  public void writeToFile(@NotNull final File out)
      throws JAXBException {
    writeToFile(out, false);
  }

  /**
   * Write the current XML to a file.
   *
   * @param out Target file
   * @param strip If true, empty elements will not be written
   * @throws JAXBException Thrown if marshalling to the target file failed
   */
  @SuppressWarnings("BooleanParameter")
  public void writeToFile(
      @NotNull final File out, final boolean strip)
      throws JAXBException {
    final Marshaller marshaller = this.jaxbContext.createMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

    if (strip) {
      final List<Passages> passages =
          this.topicPassages.getPassagesList().getPassages();
      passages.stream()
          .filter(pg -> {
            boolean state = false;
            // filter entry without any passage
            if (!pg.getP().isEmpty()) {
              // filter entry without any score
              state = pg.getP().stream()
                  .filter(p -> !p.getScore().isEmpty())
                  .findFirst().isPresent();
            }
            return state;
          }).peek(passages::remove);
    }
    marshaller.marshal(this.topicPassages, out);
  }
}
