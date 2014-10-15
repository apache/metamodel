/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.data;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.metamodel.data.Style.Color;
import org.apache.metamodel.data.Style.SizeUnit;
import org.apache.metamodel.data.Style.TextAlignment;
import org.apache.metamodel.util.EqualsBuilder;

/**
 * Builder class for {@link Style} and related objects, like {@link Color}.
 */
public final class StyleBuilder {

	private static final Map<String, Color> _colorCache = new WeakHashMap<String, Color>();

	private boolean _bold;
	private boolean _italic;
	private boolean _underline;
	private Integer _fontSize;
	private TextAlignment _alignment;
	private Color _backgroundColor;
	private Color _foregroundColor;
	private SizeUnit _fontSizeUnit;

	private final Color _defaultForegroundColor;
	private final Color _defaultBackgroundColor;

	/**
	 * Constructs a new {@link StyleBuilder} with the default foreground and
	 * background colors.
	 */
	public StyleBuilder() {
		this(createColor((short) 0, (short) 0, (short) 0), createColor(
				(short) 255, (short) 255, (short) 255));
	}

	/**
	 * Constructs a new {@link StyleBuilder} with a specified default foreground
	 * and background colors. These colors will be disregarded, if posted to the
	 * foreground and background methods.
	 * 
	 * @param defaultForegroundColor
	 * @param defaultBackgroundColor
	 */
	public StyleBuilder(Color defaultForegroundColor,
			Color defaultBackgroundColor) {
		_defaultForegroundColor = defaultForegroundColor;
		_defaultBackgroundColor = defaultBackgroundColor;
	}

	/**
	 * Resets the state of the built style, which will conceptually match it
	 * with {@link Style#NO_STYLE}.
	 */
	public void reset() {
		_bold = false;
		_italic = false;
		_underline = false;
		_fontSize = null;
		_alignment = null;
		_backgroundColor = null;
		_foregroundColor = null;
		_fontSizeUnit = null;
	}

	/**
	 * Creates a {@link Style} object based on the build characteristics.
	 * 
	 * @return a {@link Style} object based on the build characteristics.
	 */
	public Style create() {
		StyleImpl style = new StyleImpl(_bold, _italic, _underline, _fontSize,
				_fontSizeUnit, _alignment, _backgroundColor, _foregroundColor);
		if (Style.NO_STYLE.equals(style)) {
			return Style.NO_STYLE;
		}
		return style;
	}

	/**
	 * Sets the font weight to bold
	 * 
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder bold() {
		_bold = true;
		return this;
	}

	/**
	 * Sets the font style to italic
	 * 
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder italic() {
		_italic = true;
		return this;
	}

	/**
	 * Sets the text decoration to underlined
	 * 
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder underline() {
		_underline = true;
		return this;
	}

	/**
	 * Creates a Color based on a 6-letter RGB hex color code string, eg.
	 * "000000" for black and "FFFFFF" for white.
	 * 
	 * @param rgbColorCode
	 *            a 6-letter RGB hex color code string
	 * @return a color representing this color code
	 */
	public static Color createColor(String rgbColorCode) {
		assert rgbColorCode.length() == 6;
		String redParth = rgbColorCode.substring(0, 2);
		String greenParth = rgbColorCode.substring(2, 4);
		String blueParth = rgbColorCode.substring(4, 6);
		return createColor(Integer.parseInt(redParth, 16),
				Integer.parseInt(greenParth, 16),
				Integer.parseInt(blueParth, 16));
	}

	/**
	 * Creates a color based on 3 RGB components, represented as ints
	 * 
	 * @param r
	 * @param g
	 * @param b
	 * @return a color representing the RGB code
	 */
	public static Color createColor(int r, int g, int b) {
		return createColor(toRgbComponent(r), toRgbComponent(g),
				toRgbComponent(b));
	}

	/**
	 * Creates a color based on 3 RGB components, represented as shorts
	 * 
	 * @param r
	 * @param g
	 * @param b
	 * @return a color representing the RGB code
	 */
	public static Color createColor(short r, short g, short b) {
		String cacheId = r + "," + g + "," + b;
		Color color = _colorCache.get(cacheId);
		if (color == null) {
			color = new ColorImpl(r, g, b);
			_colorCache.put(cacheId, color);
		}
		return color;
	}

	private static short toRgbComponent(int r) {
		if (r < 0) {
			// if eg. a byte was passed as a RGB component
			r = (256 + r);
		}
		if (r > 255) {
			throw new IllegalArgumentException(
					"RGB component cannot be higher than 255");
		}
		return (short) r;
	}

	/**
	 * Sets the foreground (text) color of the style
	 * 
	 * @param rgbColorCode
	 *            a 6-letter hex RGB color code, such as FF0000 (red).
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder foreground(String rgbColorCode) {
		return foreground(createColor(rgbColorCode));
	}

	/**
	 * Sets the foreground (text) color of the style
	 * 
	 * @param rgb
	 *            a triplet array of shorts
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder foreground(short[] rgb) {
		assert rgb.length == 3;
		return foreground(createColor(rgb[0], rgb[1], rgb[2]));
	}

	/**
	 * Sets the foreground (text) color of the style
	 * 
	 * @param r
	 *            red amount (0-255)
	 * @param g
	 *            green amount (0-255)
	 * @param b
	 *            blue amount (0-255)
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder foreground(int r, int g, int b) {
		return foreground(createColor(r, g, b));
	}

	/**
	 * Sets the foreground (text) color of the style
	 * 
	 * @param color
	 *            the color to use
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder foreground(Color color) {
		if (EqualsBuilder.equals(_defaultForegroundColor, color)) {
			_foregroundColor = null;
		} else {
			_foregroundColor = color;
		}
		return this;
	}

	/**
	 * Sets the background (fill) color of the style
	 * 
	 * @param rgbColorCode
	 *            a 6-letter hex RGB color code, such as FF0000 (red).
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder background(String rgbColorCode) {
		return background(createColor(rgbColorCode));
	}

	/**
	 * Sets the background (fill) color of the style
	 * 
	 * @param rgb
	 *            a triplet array of shorts
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder background(short[] rgb) {
		assert rgb.length == 3;
		return background(createColor(rgb[0], rgb[1], rgb[2]));
	}

	/**
	 * Sets the background (fill) color of the style
	 * 
	 * @param r
	 *            red amount (0-255)
	 * @param g
	 *            green amount (0-255)
	 * @param b
	 *            blue amount (0-255)
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder background(int r, int g, int b) {
		return background(createColor(r, g, b));
	}

	/**
	 * Sets the background (fill) color of the style
	 * 
	 * @param color
	 *            the color to use
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder background(Color color) {
		if (EqualsBuilder.equals(_defaultBackgroundColor, color)) {
			_backgroundColor = null;
		} else {
			_backgroundColor = color;
		}
		return this;
	}

	/**
	 * Sets the font size of the style
	 * 
	 * @param fontSize
	 *            the font size
	 * @param sizeUnit
	 *            the font size unit
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder fontSize(int fontSize, SizeUnit sizeUnit) {
		_fontSize = fontSize;
		_fontSizeUnit = sizeUnit;
		return this;
	}

	/**
	 * Sets the text alignment to center
	 * 
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder centerAligned() {
		_alignment = TextAlignment.CENTER;
		return this;
	}

	/**
	 * Sets the text alignment to left
	 * 
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder leftAligned() {
		_alignment = TextAlignment.LEFT;
		return this;
	}

	/**
	 * Sets the text alignment to right
	 * 
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder rightAligned() {
		_alignment = TextAlignment.RIGHT;
		return this;
	}

	/**
	 * Sets the text alignment to justify
	 * 
	 * @return the {@link StyleBuilder} self (for cascading method calls)
	 */
	public StyleBuilder justifyAligned() {
		_alignment = TextAlignment.JUSTIFY;
		return this;
	}
}
