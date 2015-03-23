package com.sf.monitor.druid;

import retrofit.http.GET;

import java.util.List;

public interface DruidOverlordService {

  @GET("/druid/indexer/v1/workers")
  public List<MiddleManager> getWorkers();

}
