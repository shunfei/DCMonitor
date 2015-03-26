package com.sf.monitor.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: sundy
 * @since 2015-02-28.
 */
@Controller
public class IndexController {
  @RequestMapping("/hello")
  public
  @ResponseBody
  String hello() {
    return "hello world";
  }

  @RequestMapping("/")
  public ModelAndView index() {
    Map<String, Object> model = new HashMap<String, Object>();
    model.put("message", "DC_MONITOR");
    return new ModelAndView("console", model);
  }

  @RequestMapping("/kafka")
  public String kafka() {
    return "kafka";
  }

  @RequestMapping("/zk")
  public String zk() {
    return "zk";
  }

  @RequestMapping("/druid")
  public String druid() {
    return "druid";
  }

}
