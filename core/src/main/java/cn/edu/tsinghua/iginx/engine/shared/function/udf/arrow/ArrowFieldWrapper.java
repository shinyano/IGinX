package cn.edu.tsinghua.iginx.engine.shared.function.udf.arrow;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

public class ArrowFieldWrapper extends Field {

  public ArrowFieldWrapper(String name, FieldType fieldType, List<Field> children) {
    super(name, fieldType, children);
  }
}
