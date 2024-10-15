/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
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
package cn.edu.tsinghua.iginx.filesystem.service.storage;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;

import java.util.*;

import javafx.print.Collation;
import lombok.*;
import lombok.experimental.FieldNameConstants;

import javax.annotation.Nullable;

@Data
@With
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class DummyConfigs extends AbstractConfig {
//  String root;
//  String struct;
//  @Optional Config config = ConfigFactory.empty();
  Map<String, StorageConfig> dummyConfigMap;

  Map<String, List<String>> prefixMap;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
//    validateNotBlanks(problems, Fields.root, root);
//    validateNotNull(problems, Fields.struct, struct);
//    validateNotNull(problems, Fields.config, config);
//    FileStructure fileStructure = FileStructureManager.getInstance().getByName(struct);
//    if (fileStructure == null) {
//      problems.add(new ValidationProblem(Fields.struct, "Unknown file structure: " + struct));
//    } else {
//      try (Closeable shared = fileStructure.newShared(config)) {
//        // TODO: check file structure, return some warnging
//      } catch (IOException e) {
//        problems.add(new ValidationProblem(Fields.config, "Invalid config: " + e));
//      }
//    }
    validateDummyMap(problems, Fields.dummyConfigMap, dummyConfigMap);
    validatePrefixMap(problems, Fields.prefixMap, prefixMap);
    for (Map.Entry<String, List<String>> entry : prefixMap.entrySet()) {
      for (String path : entry.getValue()) {
        if (!dummyConfigMap.containsKey(path)) {
          problems.add(new ValidationProblem(Fields.prefixMap,
                  String.format("Can't find path %s in config map.", path)));
        }
      }
    }
    return problems;
  }

  protected static void validateDummyMap(
          List<ValidationProblem> dst, String field, @Nullable Map<String, StorageConfig> value) {
    if (!validateNotNull(dst, field, value)) {
      return;
    }
    assert value != null;
    if (value.isEmpty()) {
      dst.add(new ValidationWarning(field, "must not be empty"));
      return;
    }
    for (String key : value.keySet()) {
      if (key.isEmpty()) {
        dst.add(new ValidationProblem(field, "One of the keys is empty"));
        continue;
      }
      dst.addAll(value.get(key).validate());
    }
  }

  protected static void validatePrefixMap(List<ValidationProblem> dst, String field, @Nullable Map<String, List<String>> value) {
    if (!validateNotNull(dst, field, value)) {
      return;
    }
    assert value != null;
    if (value.isEmpty()) {
      dst.add(new ValidationProblem(field, "must not be empty"));
      return;
    }
    for (String key : value.keySet()) {
      if (key.isEmpty()) {
        dst.add(new ValidationProblem(field, "One of the keys is empty"));
      }
    }
  }

  public Collection<StorageConfig> getConfigs() {
    if (dummyConfigMap != null) {
      return dummyConfigMap.values();
    }
    return null;
  }
}
