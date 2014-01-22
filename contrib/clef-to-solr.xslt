<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xs="http://www.w3.org/2001/XMLSchema">

  <xsl:output
    encoding="UTF-8"
    omit-xml-declaration="yes"
    indent="yes"/>
  <xsl:strip-space elements="*"/>

  <!-- Used to transform string case -->
  <xsl:variable name="lowercase" select="'abcdefghijklmnopqrstuvwxyz'" />
  <xsl:variable name="uppercase" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ'" />

  <xsl:template match="invention-title">
    <xsl:element name="field">
      <xsl:attribute name="name">
        <xsl:value-of select="concat('title_', translate(./@lang, $uppercase, $lowercase))"/>
      </xsl:attribute>
      <xsl:value-of select="./text()"/>
      <!-- add spaces between elements -->
      <xsl:for-each select="./*">
        <xsl:value-of select="."/>
        <xsl:text> </xsl:text>
      </xsl:for-each>
    </xsl:element>
  </xsl:template>

  <xsl:template match="abstract">
    <xsl:element name="field">
      <xsl:attribute name="name">
        <xsl:value-of select="concat('abstract_', translate(./@lang, $uppercase, $lowercase))"/>
      </xsl:attribute>
      <xsl:value-of select="./text()"/>
      <!-- add spaces between elements -->
      <xsl:for-each select="./*">
        <xsl:value-of select="."/>
        <xsl:text> </xsl:text>
      </xsl:for-each>
    </xsl:element>
  </xsl:template>

  <xsl:template match="description">
    <xsl:element name="field">
      <xsl:attribute name="name">
        <xsl:value-of select="concat('description_', translate(./@lang, $uppercase, $lowercase))"/>
      </xsl:attribute>
      <xsl:value-of select="./text()"/>
      <!-- add spaces between elements -->
      <xsl:for-each select="./*">
        <xsl:value-of select="."/>
        <xsl:text> </xsl:text>
      </xsl:for-each>
    </xsl:element>
  </xsl:template>

  <xsl:template match="claims">
    <xsl:element name="field">
      <xsl:attribute name="name">
        <xsl:value-of select="concat('claims_', translate(./@lang, $uppercase, $lowercase))"/>
      </xsl:attribute>
      <!-- add spaces between elements -->
      <xsl:for-each select="./*">
        <xsl:value-of select="."/>
        <xsl:text> </xsl:text>
      </xsl:for-each>
    </xsl:element>
  </xsl:template>

  <xsl:template match="technical-data">
    <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="bibliographic-data">
    <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="patent-document">
    <xsl:element name="field">
      <xsl:attribute name="name">
        <xsl:value-of select="'id'"/>
      </xsl:attribute>
      <xsl:value-of select="./@ucid"/>
    </xsl:element>

    <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="/">
    <doc>
      <xsl:apply-templates/>
    </doc>
  </xsl:template>

  <!-- suppress unmatched elements -->
  <xsl:template match="*">
    <!--
    <xsl:message terminate="no">
      WARNING: Unmatched element: <xsl:value-of select="name()"/>
    </xsl:message>
    -->
  </xsl:template>
</xsl:stylesheet>