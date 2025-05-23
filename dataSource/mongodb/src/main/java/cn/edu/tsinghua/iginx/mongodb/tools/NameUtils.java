/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Escaper;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class NameUtils {

  private static final char NAME_SEPARATOR = '/';

  private static final ColumnKeyTranslator COLUMN_KEY_TRANSLATOR =
      new ColumnKeyTranslator(',', '=', getEscaper());

  private static Escaper getEscaper() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put(',', ',');
    replacementMap.put('=', '=');
    replacementMap.put('$', '!');
    replacementMap.put('\0', 'b');
    return new Escaper('\\', replacementMap);
  }

  public static String getCollectionName(Field field) {
    ColumnKey columnKey = new ColumnKey(field.getName(), field.getTags());
    String escapedName = COLUMN_KEY_TRANSLATOR.translate(columnKey);
    return NAME_SEPARATOR + escapedName + NAME_SEPARATOR + field.getType().name();
  }

  public static Field parseCollectionName(String collectionName) throws ParseException {
    int lastSepIndex = collectionName.lastIndexOf(NAME_SEPARATOR);
    if (collectionName.length() < 2) {
      throw new ParseException("Invalid length!", 0);
    }
    if (lastSepIndex == -1) {
      throw new ParseException("Missing separator!", 0);
    }
    if (lastSepIndex == 0) {
      throw new ParseException("Missing last separator!", 0);
    }
    if (collectionName.charAt(0) != NAME_SEPARATOR) {
      throw new IllegalArgumentException("Invalid prefix!");
    }
    String typeName = collectionName.substring(lastSepIndex + 1);
    DataType type = DataType.valueOf(typeName);
    String name = collectionName.substring(1, lastSepIndex);
    ColumnKey columnKey = COLUMN_KEY_TRANSLATOR.translate(name);
    return new Field(columnKey.getPath(), type, columnKey.getTags());
  }

  public static List<Field> match(
      Iterable<Field> fieldList, Iterable<String> patterns, TagFilter tagFilter) {
    List<Field> fields = new ArrayList<>();
    for (Field field : fieldList) {
      if (match(field.getName(), field.getTags(), patterns, tagFilter)) {
        fields.add(field);
      }
    }
    return fields;
  }

  public static boolean match(
      String columnName, Map<String, String> tags, Iterable<String> patterns, TagFilter tagFilter) {
    if (tagFilter != null && !TagKVUtils.match(tags, tagFilter)) {
      return false;
    }
    for (String pattern : patterns) {
      if (Pattern.matches(StringUtils.reformatPath(pattern), columnName)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isWildcard(String node) {
    return node.equals("*");
  }
}
