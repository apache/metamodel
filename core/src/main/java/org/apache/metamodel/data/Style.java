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

import java.io.Serializable;

/**
 * A {@link Style} represents the visual presentation ('styling') attributes of
 * a value in a {@link Row}. Styling can be used to highlight special values and
 * format the cells of eg. a spreadsheet.
 * 
 * Most datastores don't support styling, but some do. Those who do not support
 * it will just omit it.
 * 
 * Creation of {@link Style} objects is handled by the {@link StyleBuilder}
 * class.
 */
public interface Style extends Serializable {

	/**
	 * A style object used for values without styling, "unstyled" values or
	 * "neutrally styled" values.
	 */
	public static final Style NO_STYLE = new StyleImpl();

	/**
	 * Represents the text alignment of a value.
	 */
	public static enum TextAlignment {
		LEFT, RIGHT, CENTER, JUSTIFY
	}

	/**
	 * Represents a color used for value highlighting.
	 * 
	 * Creation of {@link Color} objects is handled by the static
	 * {@link StyleBuilder}.createColor(...) methods.
	 */
	public static interface Color extends Serializable {

		public short getRed();

		public short getGreen();

		public short getBlue();
	}

	/**
	 * Represents a unit of sizing elements (eg. fonts) in a {@link Style}.
	 */
	public static enum SizeUnit {
		/**
		 * Point unit
		 */
		PT,

		/**
		 * Pixel unit
		 */
		PX,

		/**
		 * Percent unit
		 */
		PERCENT
	}

	/**
	 * Determines whether or not the value is written in bold text.
	 * 
	 * @return true if text is bold
	 */
	public boolean isBold();

	/**
	 * Determines whether or not the value is written in italic text.
	 * 
	 * @return true if text is italic
	 */
	public boolean isItalic();

	/**
	 * Determines whether or not the value is written with an underline
	 * 
	 * @return true if text is underlined
	 */
	public boolean isUnderline();

	/**
	 * Gets the font size, or null if font size is unspecified.
	 * 
	 * @see SizeUnit
	 * 
	 * @return an Integer, or null
	 */
	public Integer getFontSize();

	/**
	 * Gets the unit of the font size.
	 * 
	 * @return an enum representing the font size unit used.
	 */
	public SizeUnit getFontSizeUnit();

	/**
	 * Gets the text alignment, or null if text alignment is unspecified.
	 * 
	 * @return a TextAlignment value, or null
	 */
	public TextAlignment getAlignment();

	/**
	 * Gets the foreground (text) color, or null if the color is unspecified.
	 * 
	 * @return a Color object representing the foreground color
	 */
	public Color getForegroundColor();

	/**
	 * Gets the background color, or null if the color is unspecified.
	 * 
	 * @return a Color object representing the background color
	 */
	public Color getBackgroundColor();

	/**
	 * Creates a Cascading Style Sheets (CSS) representation of this style.
	 * 
	 * @return a CSS string
	 */
	public String toCSS();
}