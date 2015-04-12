package com.sf.monitor.controllers;

import com.sf.log.Logger;
import com.sf.monitor.Resources;
import com.sf.monitor.druid.DruidInfos;
import com.sf.monitor.utils.JsonValues;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.List;

@Controller
@RequestMapping("/druid")
public class DruidController {
  private static final Logger log = new Logger(DruidController.class);
  private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  @RequestMapping(method = RequestMethod.POST,
                  value = "/emitter",
                  consumes = MediaType.APPLICATION_JSON_VALUE,
                  produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseStatus(HttpStatus.OK)
  public
  @ResponseBody
  String writeMetrics(HttpEntity<String> httpEntity) {
    try {
      Resources.fetchers.druidFetcher.writeMetrics(httpEntity.getBody());
    } catch (Exception e) {
      log.error(e, "");
    }
    return "done";
  }

  @RequestMapping(method = RequestMethod.POST,
                  value = "/trend_metrics",
                  consumes = MediaType.APPLICATION_JSON_VALUE,
                  produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseStatus(HttpStatus.OK)
  public
  @ResponseBody
  Object getTrendMetrics(HttpEntity<String> httpEntity) throws Exception {
    try {
      DruidInfos.MetricsParam param = Resources.jsonMapper.readValue(
        httpEntity.getBody(),
        DruidInfos.MetricsParam.class
      );
      param.fromDateTime = DateTime.parse(param.from, formatter);
      param.toDateTime = DateTime.parse(param.to, formatter);
      return Resources.druidInfos.getTrendData(param);
    } catch (Exception e) {
      log.error(e, "");
      return new DruidInfos.Result<List<JsonValues>>(null, false, "");
    }
  }

  @RequestMapping(method = RequestMethod.POST,
                  value = "/latest_metrics",
                  consumes = MediaType.APPLICATION_JSON_VALUE,
                  produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseStatus(HttpStatus.OK)
  public
  @ResponseBody
  Object getLatestMetrics(HttpEntity<String> httpEntity) throws Exception {
    try {
      DruidInfos.MetricsParam param = Resources.jsonMapper.readValue(
        httpEntity.getBody(),
        DruidInfos.MetricsParam.class
      );
      return Resources.druidInfos.getLatestData(param);
    } catch (Exception e) {
      log.error(e, "");
      return new DruidInfos.Result<JsonValues>(null, false, "");
    }
  }
}
