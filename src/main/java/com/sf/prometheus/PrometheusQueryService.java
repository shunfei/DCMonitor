package com.sf.prometheus;

import retrofit.http.GET;
import retrofit.http.Query;

import java.util.List;
import java.util.Map;

public interface PrometheusQueryService {

  public static class Result {
    public String status;
    public String errorType;
    public String error;
    public Data data;
  }

  public static class Data {
    public String resultType;
    public List<MetricItem> result;
  }

  public static class MetricItem {
    public Map<String, String> metric;
    public List<Object[]> values;
  }

  @GET("/api/v1/query_range")
  public Result query(
    @Query("query") String query,
    @Query("start") String start,
    @Query("end") String end,
    @Query("step") String step
  );
}
