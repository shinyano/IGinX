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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.schema;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 请求上下文中的模式注册表：callSiteId → OutputSchema 快照。
 *
 * <p>线程安全。首次 putIfAbsent 锁定快照，后续读取均返回同一快照。
 */
public class SchemaRegistry {

  private final ConcurrentHashMap<String, OutputSchema> snapshots = new ConcurrentHashMap<>();
  private final ArrowSchemaRegistry arrowSchemaRegistry = new ArrowSchemaRegistry();

  /** 获取调用点 callSiteId 对应的快照；若未注册则返回 null。 */
  public OutputSchema getSnapshot(String callSiteId) {
    return snapshots.get(callSiteId);
  }

  /** 若 callSiteId 尚无快照，则写入 schema 并返回 null（表示首次注册）； 若已有快照，则返回已有快照（不覆盖）。 */
  public OutputSchema putIfAbsent(String callSiteId, OutputSchema schema) {
    return snapshots.putIfAbsent(callSiteId, schema);
  }

  public ArrowSchemaRegistry getArrowSchemaRegistry() {
    return arrowSchemaRegistry;
  }

  public int size() {
    return snapshots.size();
  }

  public void clear() {
    snapshots.clear();
    arrowSchemaRegistry.clear();
  }
}
