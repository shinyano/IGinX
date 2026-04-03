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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.udf.AdaptiveUDFExecutor;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;

public abstract class PyUDF implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(PyUDF.class);
  private static final String LEGACY_UDF_OBJECT = "t";

  protected final BlockingQueue<PythonInterpreter> interpreters;

  protected final String moduleName;

  protected final String className;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public PyUDF(String moduleName, String className) {
    this(null, moduleName, className);
  }

  public PyUDF(BlockingQueue<PythonInterpreter> interpreters, String moduleName, String className) {
    this.interpreters = interpreters;
    this.moduleName = moduleName;
    this.className = className;
  }

  public void close(String funcName, PythonInterpreter interpreter) {
    if (interpreters != null) {
      while (!interpreters.isEmpty()) {
        PythonInterpreter queuedInterpreter = interpreters.poll();
        if (queuedInterpreter == null) {
          continue;
        }
        try {
          queuedInterpreter.exec(
              String.format("import sys; sys.modules.pop('%s', None)", moduleName));
        } catch (Exception e) {
          LOGGER.error("Remove module for legacy udf {} failed:", funcName, e);
        } finally {
          queuedInterpreter.close();
        }
      }
      return;
    }
    try {
      interpreter.exec(String.format("import sys; sys.modules.pop('%s', None)", moduleName));
    } catch (NullPointerException e) {
      LOGGER.error("Did not find module {} for function {}", moduleName, funcName);
    } catch (Exception e) {
      LOGGER.error("Remove module for udf {} failed:", funcName, e);
    }
  }

  //  protected List<List<Object>> invokePyUDF(
  //          List<List<Object>> data, List<Object> args, Map<String, Object> kvargs) {
  //    long timeout = config.getUDFTimeout();
  //    // 由于多个UDF共享interpreter，因此使用独特的对象名
  //    String obj = (moduleName + className).replace(".", "a");
  //    ThreadInterpreterManager.exec(
  //            String.format("import %s; %s = %s.%s()", moduleName, obj, moduleName, className));
  //    return ThreadInterpreterManager.invokeMethodWithTimeout(
  //            timeout, obj, UDF_FUNC, data, args, kvargs);
  //  }

  protected List<List<Object>> invokePyUDF(
      List<List<Object>> data, List<Object> args, Map<String, Object> kvargs) {
    if (interpreters != null) {
      PythonInterpreter interpreter = null;
      try {
        interpreter = interpreters.take();
        return (List<List<Object>>)
            interpreter.invokeMethod(LEGACY_UDF_OBJECT, UDF_FUNC, data, args, kvargs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted while waiting for legacy Python UDF interpreter", e);
      } catch (Exception e) {
        throw new RuntimeException("Failed to execute legacy Python UDF: " + moduleName, e);
      } finally {
        if (interpreter != null) {
          interpreters.offer(interpreter);
        }
      }
    }

    try {
      return AdaptiveUDFExecutor.getInstance()
          .submitAndGet(
              () -> {
                long timeout = config.getUDFTimeout();
                String obj = (moduleName + className).replace(".", "a");
                ThreadInterpreterManager.exec(
                    String.format(
                        "import %s; %s = %s.%s()", moduleName, obj, moduleName, className));
                return ThreadInterpreterManager.invokeMethodWithTimeout(
                    timeout, obj, UDF_FUNC, data, args, kvargs);
              });
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute Python UDF: " + moduleName, e);
    }
  }
}
