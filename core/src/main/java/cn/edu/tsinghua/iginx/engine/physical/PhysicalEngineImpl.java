/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.migration.MigrationPhysicalExecutor;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalEngineImpl implements PhysicalEngine {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(PhysicalEngineImpl.class);

  private static final PhysicalEngineImpl INSTANCE = new PhysicalEngineImpl();

  private final PhysicalOptimizer optimizer;

  private final MemoryPhysicalTaskDispatcher memoryTaskExecutor;

  private final StoragePhysicalTaskExecutor storageTaskExecutor;

  private PhysicalEngineImpl() {
    optimizer =
        PhysicalOptimizerManager.getInstance()
            .getOptimizer(ConfigDescriptor.getInstance().getConfig().getPhysicalOptimizer());
    memoryTaskExecutor = MemoryPhysicalTaskDispatcher.getInstance();
    storageTaskExecutor = StoragePhysicalTaskExecutor.getInstance();
    storageTaskExecutor.init(memoryTaskExecutor, optimizer.getReplicaDispatcher());
    memoryTaskExecutor.startDispatcher();
  }

  public static PhysicalEngineImpl getInstance() {
    return INSTANCE;
  }

  @Override
  public RowStream execute(RequestContext ctx, Operator root) throws PhysicalException {
    if (OperatorType.isGlobalOperator(root.getType())) { // 全局任务临时兼容逻辑
      // 迁移任务单独处理
      if (root.getType() == OperatorType.Migration) {
        return MigrationPhysicalExecutor.getInstance().execute((Migration) root, storageTaskExecutor);
      } else {
        GlobalPhysicalTask task = new GlobalPhysicalTask(root);
        TaskExecuteResult result = storageTaskExecutor.executeGlobalTask(task);
        if (result.getException() != null) {
          throw result.getException();
        }
        return result.getRowStream();
      }
    }
    PhysicalTask task = optimizer.optimize(root);
    ctx.setPhysicalTree(task);
    List<StoragePhysicalTask> storageTasks = new ArrayList<>();
    getStorageTasks(storageTasks, task);
    storageTaskExecutor.commit(storageTasks);
    TaskExecuteResult result = task.getResult();
    if (result.getException() != null) {
      throw result.getException();
    }
    return result.getRowStream();
  }

  private void getStorageTasks(List<StoragePhysicalTask> tasks, PhysicalTask root) {
    if (root == null) {
      return;
    }
    if (root.getType() == TaskType.Storage) {
      tasks.add((StoragePhysicalTask) root);
    } else if (root.getType() == TaskType.BinaryMemory) {
      BinaryMemoryPhysicalTask task = (BinaryMemoryPhysicalTask) root;
      getStorageTasks(tasks, task.getParentTaskA());
      getStorageTasks(tasks, task.getParentTaskB());
    } else if (root.getType() == TaskType.UnaryMemory) {
      UnaryMemoryPhysicalTask task = (UnaryMemoryPhysicalTask) root;
      getStorageTasks(tasks, task.getParentTask());
    } else if (root.getType() == TaskType.MultipleMemory) {
      MultipleMemoryPhysicalTask task = (MultipleMemoryPhysicalTask) root;
      for (PhysicalTask parentTask : task.getParentTasks()) {
        getStorageTasks(tasks, parentTask);
      }
    }
  }

  @Override
  public ConstraintManager getConstraintManager() {
    return optimizer.getConstraintManager();
  }

  @Override
  public StorageManager getStorageManager() {
    return storageTaskExecutor.getStorageManager();
  }
}
