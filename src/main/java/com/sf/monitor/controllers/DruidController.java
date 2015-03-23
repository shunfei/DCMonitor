package com.sf.monitor.controllers;

import com.sf.log.Logger;
import com.sf.monitor.Resources;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
@RequestMapping("/druid")
public class DruidController {
  private static final Logger log = new Logger(DruidController.class);

  @RequestMapping(method = RequestMethod.POST,
                  value = "/emitter",
                  consumes = MediaType.APPLICATION_JSON_VALUE,
                  produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseStatus(HttpStatus.OK)
  public @ResponseBody
  String writeMetrics(HttpEntity<String> httpEntity) {
    try {
      Resources.fetchers.druidFetcher.writeMetrics(httpEntity.getBody());
    }catch (Exception e){
    log.error(e, "");
  }
  return "done";
  }
}
