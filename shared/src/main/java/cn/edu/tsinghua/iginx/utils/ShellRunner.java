package cn.edu.tsinghua.iginx.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ShellRunner {

  // to run .sh script on WindowsOS in github action tests
  public static final String SH_PATH = "C:/Program Files/Git/git-bash.exe";

  public void runShellCommand(String command) throws Exception {
    Process p = null;
    try {
      ProcessBuilder builder = new ProcessBuilder();
      if (isOnOs("win")) {;
        builder.command(SH_PATH, "-c", command);
      } else {
        builder.command(command);
      }
      builder.redirectErrorStream(true);
      p = builder.start();
      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }

      int status = p.waitFor();
      System.err.printf("runShellCommand: %s, status: %s%n, %s%n", command, p.exitValue(), status);
      if (p.exitValue() != 0) {
        throw new Exception("tests fail!");
      }
    } finally {
      if (p != null) {
        p.destroy();
      }
    }
  }

  private boolean isOnOs(String osName) {
    return System.getProperty("os.name").toLowerCase().contains(osName.toLowerCase());
  }
}
