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
package cn.edu.tsinghua.iginx.filestore.service.rpc.client;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool.TTransportPoolConfig;
import com.typesafe.config.Optional;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class ClientConfig extends AbstractConfig {

  @Optional Duration socketTimeout = Duration.ZERO;
  @Optional Duration connectTimeout = Duration.ZERO;
  @Optional TTransportPoolConfig connectPool = new TTransportPoolConfig();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = Collections.emptyList();
    validateNotNull(problems, Fields.socketTimeout, socketTimeout);
    validateNotNull(problems, Fields.connectTimeout, connectTimeout);
    validateSubConfig(problems, Fields.connectPool, connectPool);
    return problems;
  }
}