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

import de.unihildesheim.iw.xml.elements.Passage;
import de.unihildesheim.iw.xml.elements.PassagesGroup;
import de.unihildesheim.iw.xml.elements.TopicPassages;
import de.unihildesheim.iw.util.StringUtils;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Jens Bertram
 */
public class TopicsXMLReader {

  /**
   * Default number of passages expected per group. Used as array initializer.
   */
  private static final int DEFAULT_PASSAGE_COUNT = 15;
  /**
   * XML context.
   */
  private final JAXBContext jaxbContext;
  private final TopicPassages topicPassages;
  private final Set<String> languages;

  /**
   * Creates a new instance from the given file.
   *
   * @param source Topics XML file
   * @throws JAXBException Thrown on unmarshalling errors
   */
  public TopicsXMLReader(final File source)
      throws JAXBException {
    this.jaxbContext = JAXBContext.newInstance(TopicPassages.class);
    final Unmarshaller jaxbUnmarshaller = this.jaxbContext.createUnmarshaller();
    this.topicPassages = (TopicPassages) jaxbUnmarshaller.unmarshal
        (source);
    this.languages = extractLanguages();
  }

  /**
   * Extract all languages available in the topicsXML file.
   *
   * @return List of languages
   */
  private Set<String> extractLanguages() {
    final Set<String> lang = new HashSet<>(10);
    for (final PassagesGroup pg : this.topicPassages.getPassageGroups()) {
      lang.addAll(pg.getPassages().stream()
          .map(p -> StringUtils.lowerCase(p.getLanguage()))
          .collect(Collectors.toList()));
    }
    return lang;
  }

  JAXBContext getJaxbContext() {
    return this.jaxbContext;
  }

  TopicPassages getTopicPassages() {
    return this.topicPassages;
  }

  public Collection<PassagesGroup> getPassagesGroups() {
    return this.topicPassages.getPassageGroups();
  }

  /**
   * Get the {@link Passage Passages} by language.
   *
   * @param lang Language
   * @return List of passages
   */
  public List<Passage> getPassages(final String lang) {
    final String language = StringUtils.lowerCase(lang);
    if (!getLanguages().contains(language)) {
      throw new IllegalArgumentException(
          "Language '" + language + "' not found.");
    }

    final Collection<PassagesGroup> sources =
        this.topicPassages.getPassageGroups();
    final List<Passage> pList = new ArrayList<>(
        sources.size() * DEFAULT_PASSAGE_COUNT);

    for (final PassagesGroup pg : sources) {
      pList.addAll(pg.getPassages().stream()
          .filter(p -> language.equals(StringUtils.lowerCase(p.getLanguage())))
          .collect(Collectors.toList()));
    }

    return pList;
  }

  /**
   * Get the languages for which topics are available in the document.
   *
   * @return List of languages
   */
  public Set<String> getLanguages() {
    return Collections.unmodifiableSet(this.languages);
  }
}
