<?xml version="1.0" encoding="utf-8"?>
<!-- An order becomes an invoice: one position per line with its amount (qty x price), and the total. -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" indent="no" omit-xml-declaration="yes"/>

  <xsl:template match="/order">
    <invoice order="{@id}" customer="{customer}">
      <xsl:attribute name="total">
        <xsl:call-template name="total">
          <xsl:with-param name="lines" select="line"/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:for-each select="line">
        <position sku="{@sku}" qty="{@qty}" amount="{@qty * @price}"/>
      </xsl:for-each>
    </invoice>
  </xsl:template>

  <!-- XSLT 1.0 has no sum of products: add the lines one by one. -->
  <xsl:template name="total">
    <xsl:param name="lines"/>
    <xsl:param name="running" select="0"/>
    <xsl:choose>
      <xsl:when test="$lines">
        <xsl:call-template name="total">
          <xsl:with-param name="lines" select="$lines[position() &gt; 1]"/>
          <xsl:with-param name="running" select="$running + $lines[1]/@qty * $lines[1]/@price"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$running"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
