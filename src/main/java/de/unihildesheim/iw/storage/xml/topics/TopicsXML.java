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
import de.unihildesheim.iw.storage.xml.topics.Languages.Lang;
import de.unihildesheim.iw.storage.xml.topics.PassagesList.Passages;
import de.unihildesheim.iw.storage.xml.topics.Scorers.Scorer;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static de.unihildesheim.iw.storage.xml.topics.Meta.Data;

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

  public enum MetaTags {
    QUERY_TYPE("queryType"), IDX_FIELDS("idxFields"), TIMESTAMP("timestamp"),
    SAMPLE("sampleId");

    final String name;

    MetaTags(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

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
      final Unmarshaller jaxbUnmarshaller =
          this.jaxbContext.createUnmarshaller();
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
    this(null);
  }

  public TopicPassages getRoot() {
    return this.topicPassages;
  }

  /**
   * Add a scorer.
   *
   * @param csc Clarity scorer
   * @param conf Scorer configuration
   */
  public void addScorer(
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

  public void setMeta(
      @NotNull final MetaTags name,
      @NotNull final String value) {
    setMeta(name.toString(), value);
  }

  /**
   * Set a meta-data value.
   *
   * @param name Value identifier
   * @param value Value
   */
  public void setMeta(
      @NotNull final String name,
      @NotNull final String value) {
    Meta meta = this.topicPassages.getMeta();
    if (meta == null) {
      meta = new Meta();
      this.topicPassages.setMeta(meta);
    }
    final Data data = new Data();
    data.setName(name);
    data.setValue(value);
    meta.getData().add(data);
  }

  public Meta getMeta() {
    Meta meta = this.topicPassages.getMeta();
    if (meta == null) {
      meta = new Meta();
      this.topicPassages.setMeta(meta);
    }
    return meta;
  }

  /**
   * Set stopwords for a single language.
   *
   * @param lang Language
   * @param words Stopwords
   */
  public void setStopwords(
      @NotNull final String lang,
      @NotNull final Collection<String> words) {
    Languages lng = this.topicPassages.getLanguages();
    if (lng == null) {
      lng = new Languages();
      this.topicPassages.setLanguages(lng);
    }
    final Optional<Lang> entry = lng.getLang().stream()
        .filter(l -> l.getName().equalsIgnoreCase(lang))
        .findFirst();

    final Lang target;
    if (entry.isPresent()) {
      target = entry.get();
    } else {
      target = new Lang();
      target.setName(lang);
    }
    target.setStopwords(StringUtils.join(words, " "));
  }
}
