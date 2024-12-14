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
package cn.edu.tsinghua.iginx.engine.shared.function.manager;

import java.io.File;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.validation.constraints.NotNull;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

/**
 * 管理线程上的interpreter，存到ThreadLocal中以便随时随地取用
 *
 * <p>不管理interpreter的生命周期，生命周期管理在AbstractTaskThreadPoolExecutor中
 */
public class ThreadInterpreterManager {
  private static final ThreadLocal<PythonInterpreter> interpreterThreadLocal = new ThreadLocal<>();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PATH =
          String.join(File.separator, config.getDefaultUDFDir(), "python_scripts");

  @NotNull
  public static PythonInterpreter getInterpreter() {
    PythonInterpreter interpreter = interpreterThreadLocal.get();
    if (interpreter == null) {
      String pythonCMD = config.getPythonCMD();
      PythonInterpreterConfig config =
              PythonInterpreterConfig.newBuilder().setPythonExec(pythonCMD).addPythonPaths(PATH).build();
      setInterpreter(new PythonInterpreter(config));
    }
    return interpreterThreadLocal.get();
  }

  public static void setInterpreter(@NotNull PythonInterpreter interpreter) {
    interpreterThreadLocal.set(interpreter);
  }

  public static void executeWithInterpreter(Consumer<PythonInterpreter> action) {
    PythonInterpreter interpreter = getInterpreter();
    action.accept(interpreter);
  }

  public static <T> T executeWithInterpreterAndReturn(Function<PythonInterpreter, T> action) {
    PythonInterpreter interpreter = getInterpreter();
    return action.apply(interpreter);
  }

  /** 从threadlocal中移除interpreter */
  public static void cleanupInterpreter() {
    PythonInterpreter interpreter = interpreterThreadLocal.get();
    if (interpreter != null) {
      interpreterThreadLocal.remove();
    }
  }
}
