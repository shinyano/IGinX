package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.CancelTransformJobReq;
import cn.edu.tsinghua.iginx.thrift.Status;

public class AlterEngineStatement extends SystemStatement{

  private final long engineId;

  private final IginxWorker worker = IginxWorker.getInstance();

  public AlterEngineStatement(long engineId) {
    this.statementType = StatementType.ALTER_ENGINE;
    this.engineId = engineId;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    CancelTransformJobReq req = new CancelTransformJobReq(ctx.getSessionId(), engineId);
    Status status = worker.cancelTransformJob(req);

    Result result = new Result(status);
    ctx.setResult(result);
  }
}
