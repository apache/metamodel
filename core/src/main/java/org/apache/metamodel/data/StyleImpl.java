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

import java.util.List;

import org.apache.metamodel.util.BaseObject;

/**
 * Default immutable implementation of {@link Style}.
 */
public final class StyleImpl extends BaseObject implements Style {

    private static final long serialVersionUID = 1L;

    private final boolean _underline;
    private final boolean _italic;
    private final boolean _bold;
    private final Integer _fontSize;
    private final TextAlignment _alignment;
    private final Color _backgroundColor;
    private final Color _foregroundColor;
    private final SizeUnit _fontSizeUnit;

    public StyleImpl() {
        this(false, false, false, null, null, null, null, null);
    }

    public StyleImpl(boolean bold, boolean italic, boolean underline, Integer fontSize, SizeUnit fontSizeUnit,
            TextAlignment alignment, Color backgroundColor, Color foregroundColor) {
        _bold = bold;
        _italic = italic;
        _underline = underline;
        _fontSize = fontSize;
        _fontSizeUnit = fontSizeUnit;
        _alignment = alignment;
        _backgroundColor = backgroundColor;
        _foregroundColor = foregroundColor;
    }

    @Override
    public boolean isBold() {
        return _bold;
    }

    @Override
    public boolean isItalic() {
        return _italic;
    }

    @Override
    public boolean isUnderline() {
        return _underline;
    }

    @Override
    public Integer getFontSize() {
        return _fontSize;
    }

    @Override
    public SizeUnit getFontSizeUnit() {
        return _fontSizeUnit;
    }

    @Override
    public TextAlignment getAlignment() {
        return _alignment;
    }

    @Override
    public Color getForegroundColor() {
        return _foregroundColor;
    }

    @Override
    public Color getBackgroundColor() {
        return _backgroundColor;
    }

    @Override
    public String toCSS() {
        StringBuilder sb = new StringBuilder();
        if (_bold) {
            sb.append("font-weight: bold;");
        }
        if (_italic) {
            sb.append("font-style: italic;");
        }
        if (_underline) {
            sb.append("text-decoration: underline;");
        }
        if (_alignment != null) {
            sb.append("text-align: " + toCSS(_alignment) + ";");
        }
        if (_fontSize != null) {
            sb.append("font-size: " + _fontSize);
            switch (_fontSizeUnit) {
            case PT:
                sb.append("pt");
                break;
            case PX:
                sb.append("px");
                break;
            case PERCENT:
                sb.append("%");
                break;
            default:
                // don't write a size unit
            }
            sb.append(';');
        }
        if (_foregroundColor != null) {
            sb.append("color: " + toCSS(_foregroundColor) + ";");
        }
        if (_backgroundColor != null) {
            sb.append("background-color: " + toCSS(_backgroundColor) + ";");
        }
        return sb.toString();
    }

    private String toCSS(Color c) {
        return "rgb(" + c.getRed() + "," + c.getGreen() + "," + c.getBlue() + ")";
    }

    @Override
    public String toString() {
        return toCSS();
    }

    private String toCSS(TextAlignment alignment) {
        switch (alignment) {
        case LEFT:
            return "left";
        case RIGHT:
            return "right";
        case CENTER:
            return "center";
        case JUSTIFY:
            return "justify";
        default:
            throw new IllegalStateException("Unknown alignment: " + alignment);
        }
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_underline);
        identifiers.add(_italic);
        identifiers.add(_bold);
        identifiers.add(_fontSize);
        identifiers.add(_fontSizeUnit);
        identifiers.add(_alignment);
        identifiers.add(_backgroundColor);
        identifiers.add(_foregroundColor);
    }
}
