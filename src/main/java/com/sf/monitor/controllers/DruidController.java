package com.sf.monitor.controllers;

import com.sf.log.Logger;
import com.sf.monitor.Event;
import com.sf.monitor.Resources;
import com.sf.monitor.druid.DruidInfos;
import com.sf.monitor.kafka.KafkaStats;
import com.sf.monitor.utils.JsonValues;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/druid")
public class DruidController {
  private static final Logger log = new Logger(DruidController.class);
  private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  @Value("${druid.query.time.offset:PT15m}")
  private String timeOffset;

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

  @RequestMapping("/detail")
  public
  @ResponseBody
  Map<String, List<Event>> topicDetail(String topic, String consumer, String from, String to, Integer partitionId) {
    DateTime fromDate = null;
    DateTime toDate = null;
    DateTime now = new DateTime();
    try {
      fromDate = DateTime.parse(from, formatter);
    } catch (Exception e) {
      fromDate = now;
    }
    try {
      toDate = DateTime.parse(to, formatter);
    } catch (Exception e) {
      toDate = now;
    }
    if (from == null || to.equals(from)) {
      fromDate = toDate.minus(new Period(timeOffset));
    }
    partitionId = (partitionId == null) ? -1 : partitionId;
    return KafkaStats.getTrendConsumeInfos(consumer, topic, partitionId, fromDate, toDate);
  }

  @RequestMapping("/trend_metrics")
  public
  @ResponseBody
  Map<String, List<Event>> getTrendMetrics(HttpServletRequest req) throws Exception {
    String from = req.getParameter("from");
    String to = req.getParameter("to");

    DateTime fromTime = null;
    DateTime toTime = null;
    DateTime now = new DateTime();
    try {
      fromTime = DateTime.parse(from, formatter);
    } catch (Exception e) {
      fromTime = now;
    }
    try {
      toTime = DateTime.parse(to, formatter);
    } catch (Exception e) {
      toTime = now;
    }
    if (from == null || to.equals(from)) {
      fromTime = toTime.minus(new Period(timeOffset));
    }

    Map<String, String> tags = new HashMap<>();
    for (Map.Entry<String, String[]> e : req.getParameterMap().entrySet()) {
      if (!"from".equalsIgnoreCase(e.getKey())
          && !"to".equalsIgnoreCase(e.getKey())
          && e.getValue() != null
          && e.getValue().length > 0) {
        tags.put(e.getKey(), e.getValue()[0]);
      }
    }

    return Resources.druidInfos.getTrendData(tags, fromTime, toTime);
  }

  @RequestMapping("/realtime_nodes")
  public
  @ResponseBody
  List<JsonValues> getRealtimeNodes() {
    return Resources.druidInfos.getRealtimeNodes();
  }

  @RequestMapping("/historical_nodes")
  public
  @ResponseBody
  List<JsonValues> getHistoricalNodes() {
    return Resources.druidInfos.getHistoricalNodes();
  }

  @RequestMapping("/middle_manager_nodes")
  public
  @ResponseBody
  List<JsonValues> getMiddleManagerNodes() {
    return Resources.druidInfos.getMiddleManagerNodes();
  }

  @RequestMapping("/broker_nodes")
  public
  @ResponseBody
  Iterable<Map<String, Object>> getBrokerNodes() {
    return DruidInfos.AnnounceNode.toMaps(Resources.druidInfos.getBrokerNodes());
  }

  @RequestMapping("/overlord_nodes")
  public
  @ResponseBody
  Iterable<Map<String, Object>> getOverlordNodes() {
    return DruidInfos.AnnounceNode.toMaps(Resources.druidInfos.getOverlordNodes());
  }

  @RequestMapping("/coordinator_nodes")
  public
  @ResponseBody
  Iterable<Map<String, Object>> getCoordinatorNodes() {
    return DruidInfos.AnnounceNode.toMaps(Resources.druidInfos.getCoordinatorNodes());
  }

}
