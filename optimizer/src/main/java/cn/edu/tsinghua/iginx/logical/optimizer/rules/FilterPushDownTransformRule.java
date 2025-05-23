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
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.auto.service.AutoService;
import java.util.*;

@AutoService(Rule.class)
public class FilterPushDownTransformRule extends Rule {

  static final Set<Class> validTransforms =
      new HashSet<>(Arrays.asList(MappingTransform.class, RowTransform.class, SetTransform.class));

  public FilterPushDownTransformRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *   Row/Set/MappingTransform
     */
    super(
        "FilterPushDownTransformRule",
        "FilterPushDownRule",
        operand(Select.class, operand(AbstractUnaryOperator.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) ((OperatorSource) select.getSource()).getOperator();

    if (!validTransforms.contains(operator.getClass())) {
      return false;
    }

    return LogicalFilterUtils.splitFilter(select.getFilter()).stream()
        .anyMatch(filter -> !filterHasFunction(filter, getFuncionCallList(operator)));
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) ((OperatorSource) select.getSource()).getOperator();

    List<FunctionCall> functionCallList = getFuncionCallList(operator);
    List<Filter> splitFilter = LogicalFilterUtils.splitFilter(select.getFilter());

    List<Filter> remainFilters = new ArrayList<>(), pushDownFilters = new ArrayList<>();
    splitFilter.forEach(
        child -> {
          if (filterHasFunction(child, functionCallList)) {
            remainFilters.add(child);
          } else {
            pushDownFilters.add(child);
          }
        });

    select.setFilter(new AndFilter(remainFilters));
    Select pushDownSelect =
        new Select(operator.getSource(), new AndFilter(pushDownFilters), select.getTagFilter());
    operator.setSource(new OperatorSource(pushDownSelect));
    call.transformTo(operator);
  }

  private List<FunctionCall> getFuncionCallList(AbstractUnaryOperator operator) {
    if (operator instanceof MappingTransform) {
      return ((MappingTransform) operator).getFunctionCallList();
    } else if (operator instanceof RowTransform) {
      return ((RowTransform) operator).getFunctionCallList();
    } else if (operator instanceof SetTransform) {
      return ((SetTransform) operator).getFunctionCallList();
    } else {
      throw new IllegalArgumentException(
          "operator should be MappingTransform, RowTransform or SetTransform");
    }
  }

  /**
   * 判断filter中是否含有给定函数（FunctionCall）列表中的函数
   *
   * @param filter 给定filter
   * @param functionCallList 给定函数列表
   * @return filter中是否含有给定函数
   */
  private boolean filterHasFunction(Filter filter, List<FunctionCall> functionCallList) {
    final boolean[] hasFunction = {false};

    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {
            hasFunction[0] |= isFunc(filter.getPath(), functionCallList);
          }

          @Override
          public void visit(PathFilter filter) {
            hasFunction[0] |= isFunc(filter.getPathA(), functionCallList);
            hasFunction[0] |= isFunc(filter.getPathB(), functionCallList);
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            hasFunction[0] |= expressionHasFunction(filter.getExpressionA(), functionCallList);
            hasFunction[0] |= expressionHasFunction(filter.getExpressionB(), functionCallList);
          }

          @Override
          public void visit(InFilter filter) {
            hasFunction[0] |= isFunc(filter.getPath(), functionCallList);
          }
        });

    return hasFunction[0];
  }

  /**
   * 判断Expression中是否含有给定函数（Function Call）列表中的函数
   *
   * @param expression 给定Expression
   * @param functionCallList 给定函数列表
   * @return Expression中是否含有给定函数
   */
  private boolean expressionHasFunction(
      Expression expression, List<FunctionCall> functionCallList) {
    final boolean[] hasFunction = {false};
    expression.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            hasFunction[0] |= isFunc(expression.getColumnName(), functionCallList);
          }

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(BinaryExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {
            hasFunction[0] |= isFunc(expression.getColumnName(), functionCallList);
          }

          @Override
          public void visit(MultipleExpression expression) {}

          @Override
          public void visit(CaseWhenExpression expression) {}

          @Override
          public void visit(KeyExpression expression) {}

          @Override
          public void visit(SequenceExpression expression) {}
        });

    return hasFunction[0];
  }

  public static boolean isFunc(String path, List<FunctionCall> functionCallList) {
    for (FunctionCall functionCall : functionCallList) {
      if (functionCall.getFunctionStr().equals(path)) {
        return true;
      } else if (functionCall.getFunctionStr().contains("*")
          && path.matches(StringUtils.reformatPath(functionCall.getFunctionStr()))) {
        return true;
      }
    }

    return false;
  }
}
