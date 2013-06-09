/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.excel;

import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.eobjects.metamodel.data.Style;
import org.eobjects.metamodel.data.Style.Color;
import org.eobjects.metamodel.data.Style.SizeUnit;
import org.eobjects.metamodel.data.Style.TextAlignment;
import org.eobjects.metamodel.insert.AbstractRowInsertionBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.LazyRef;

/**
 * {@link RowInsertionBuilder} for excel spreadsheets.
 * 
 * @author Kasper Sørensen
 */
final class ExcelInsertBuilder extends
		AbstractRowInsertionBuilder<ExcelUpdateCallback> {

	public ExcelInsertBuilder(ExcelUpdateCallback updateCallback, Table table) {
		super(updateCallback, table);
	}

	@Override
	public void execute() {
		final Object[] values = getValues();
		final Style[] styles = getStyles();

		final Row row = getUpdateCallback().createRow(getTable().getName());

		final Column[] columns = getColumns();
		for (int i = 0; i < columns.length; i++) {
			Object value = values[i];
			if (value != null) {
				int columnNumber = columns[i].getColumnNumber();
				Cell cell = row.createCell(columnNumber);

				// use a lazyref and the isFetched method to only create style
				// if nescesary
				LazyRef<CellStyle> cellStyle = new LazyRef<CellStyle>() {
					@Override
					protected CellStyle fetch() {
						return getUpdateCallback().createCellStyle();
					}
				};

				if (value instanceof Number) {
					cell.setCellValue(((Number) value).doubleValue());
				} else if (value instanceof Boolean) {
					cell.setCellValue((Boolean) value);
				} else if (value instanceof Date) {
					cell.setCellValue((Date) value);
				} else {
					cell.setCellValue(value.toString());
				}

				Style style = styles[i];
				if (style != null && !Style.NO_STYLE.equals(style)) {
					LazyRef<Font> font = new LazyRef<Font>() {
						@Override
						protected Font fetch() {
							return getUpdateCallback().createFont();
						}

					};
					if (style.isBold()) {
						font.get().setBoldweight(Font.BOLDWEIGHT_BOLD);
					}
					if (style.isItalic()) {
						font.get().setItalic(true);
					}
					if (style.isUnderline()) {
						font.get().setUnderline(Font.U_SINGLE);
					}
					if (style.getFontSize() != null) {
						Integer fontSize = style.getFontSize();
						SizeUnit sizeUnit = style.getFontSizeUnit();
						if (sizeUnit == SizeUnit.PERCENT) {
							fontSize = convertFontPercentageToPt(fontSize);
						}
						font.get().setFontHeightInPoints(fontSize.shortValue());
					}
					Color foregroundColor = style.getForegroundColor();
					if (foregroundColor != null) {
						short index = getUpdateCallback().getColorIndex(
								foregroundColor);
						font.get().setColor(index);
					}
					if (font.isFetched()) {
						cellStyle.get().setFont(font.get());
					}
					if (style.getAlignment() != null) {
						cellStyle.get().setAlignment(
								getAlignment(style.getAlignment()));
					}

					final Color backgroundColor = style.getBackgroundColor();
					if (backgroundColor != null) {
						cellStyle.get().setFillPattern(
								CellStyle.SOLID_FOREGROUND);
						cellStyle.get().setFillForegroundColor(
								getUpdateCallback().getColorIndex(
										backgroundColor));
					}
				}

				if (value instanceof Date) {
					if (cellStyle.isFetched()) {
						cellStyle.get().setDataFormat(
								getUpdateCallback().getDateCellFormat());
					} else {
						cellStyle = new LazyRef<CellStyle>() {
							@Override
							protected CellStyle fetch() {
								return getUpdateCallback().getDateCellStyle();
							}
						};
						// trigger the fetch
						cellStyle.get();
					}
				}

				if (cellStyle.isFetched()) {
					cell.setCellStyle(cellStyle.get());
				}
			}
		}
	}

	/**
	 * Converts a percentage based font size to excel "pt" scale.
	 * 
	 * @param percentage
	 * @return
	 */
	private Integer convertFontPercentageToPt(Integer percentage) {
		Double d = percentage.intValue() * 11.0 / 100;
		return d.intValue();
	}

	private short getAlignment(TextAlignment alignment) {
		switch (alignment) {
		case LEFT:
			return CellStyle.ALIGN_LEFT;
		case RIGHT:
			return CellStyle.ALIGN_RIGHT;
		case CENTER:
			return CellStyle.ALIGN_CENTER;
		case JUSTIFY:
			return CellStyle.ALIGN_JUSTIFY;
		default:
			throw new IllegalArgumentException("Unknown alignment type: "
					+ alignment);
		}
	}
}
