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

package de.unihildesheim.iw.storage.xml.topics;

import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.iw.util.Configuration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
public final class TopicsXML {
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
   * @throws JAXBException Thrown on context errors
   */
  public TopicsXML(@Nullable final File source)
      throws JAXBException {
    this.jaxbContext = JAXBContext
        .newInstance(TopicPassages.class);

    if (source == null || !source.exists()) {
      this.topicPassages = new TopicPassages();
    } else {
      final Unmarshaller jaxbUnmarshaller = this.jaxbContext.createUnmarshaller();
      this.topicPassages = (TopicPassages) jaxbUnmarshaller.unmarshal(source);
    }
  }

  /**
   * Creates a new empty instance.
   *
   * @throws JAXBException Thrown on context errors
   */
  public TopicsXML()
      throws JAXBException {
    this.jaxbContext = JAXBContext
        .newInstance(TopicPassages.class);
    this.topicPassages = new TopicPassages();
  }

  public TopicPassages getRoot() {
    return this.topicPassages;
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
    final Scorers.Scorer s = new Scorers.Scorer();
    s.setImpl(csc.getIdentifier());

    Scorers scorers = this.topicPassages.getScorers();
    if (scorers == null) {
      this.topicPassages.setScorers(new Scorers());
      scorers = this.topicPassages.getScorers();
    }
    scorers.getScorer().add(s);
  }

  /**
   * Get a list of {@link PassagesListEntry Passages entries} by language.
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
   * Get the {@link PassagesList}.
   *
   * @return List of passages
   */
  @Nullable
  public final PassagesList getPassagesList() {
    return this.topicPassages.getPassagesList();
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
  public final void writeToFile(
      @NotNull final File out, final boolean strip)
      throws JAXBException {
    final Marshaller marshaller = this.jaxbContext.createMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

    if (strip) {
      final List<PassagesList.Passages> passages =
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
