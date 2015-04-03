package com.sf.monitor.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

public class TagValue {
  public static final String In = "in";
  public static final String NotIn = "notin";
  public static final String Equal = "=";
  public static final String NotEqual = "!=";
  public static final String Greater = ">";
  public static final String GreaterEqual = ">=";
  public static final String Less = "<";
  public static final String LessEqaul = "<=";


  @JsonProperty
  public String tag;
  @JsonProperty
  public Object value;
  @JsonProperty
  public String cmp;

  @JsonCreator
  public TagValue(
    @JsonProperty("tag") String tag,
    @JsonProperty("cmp") String cmp,
    @JsonProperty("value") Object value
  ) {
    this.tag = tag;
    this.value = value;
    this.cmp = (cmp == null) ? Equal : cmp;
  }

  public String toSql() {
    if (In.equals(cmp) || NotIn.equals(cmp)) {
      String joinLogic = In.equals(cmp) ? " or " : " and ";
      final String realCmp = In.equals(cmp) ? Equal : NotEqual;
      return Joiner.on(joinLogic).join(
        Lists.transform(
          (List) value, new Function() {
            @Override
            public Object apply(Object input) {
              return String.format("%s %s '%s'", tag, realCmp, input.toString());
            }
          }
        )
      );
    }else {
      return String.format("%s %s '%s'", tag, cmp, value);
    }
  }
}
