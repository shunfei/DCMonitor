package com.sf.monitor.druid;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sf.influxdb.dto.Point;
import com.sf.log.Logger;
import com.sf.monitor.CommonFetcher;
import com.sf.monitor.InfoFetcher;
import com.sf.monitor.Resources;

import java.util.List;

public class DruidInfoFetcher extends CommonFetcher {
  private static final Logger log = new Logger(DruidInfoFetcher.class);

  @JsonProperty
  public String fetchPeriod;

  public String type() {
    return InfoFetcher.Druid;
  }

  public void writeMetrics(String msg) throws Exception {
    log.debug(msg);

    List<EmitMetricsAnalyzer.MetricInfo> rawInfos = Resources.jsonMapper.readValue(
      msg, new TypeReference<List<EmitMetricsAnalyzer.MetricInfo>>() {
      }
    );
    List<Point> points = EmitMetricsAnalyzer.fetchReceivedInfos(rawInfos);

    if (log.isDebugEnabled()){
      log.debug(Resources.jsonMapper.writeValueAsString(points));
    }
    saveMetrics(points);

    if (rawInfos.size() > 0) {
      log.debug("writeMetrics host: [%s]", rawInfos.get(0).host);
    }
  }

  @Override
  public void start() throws Exception {
    // Do nothing.
    log.info("%s started!", DruidInfoFetcher.class.getSimpleName());
  }

  @Override
  public void stop() {
    // Do nothing.
    log.info("%s stopped!", DruidInfoFetcher.class.getSimpleName());
  }
}
