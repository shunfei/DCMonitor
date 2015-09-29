package com.sf.monitor.controllers;

import io.prometheus.client.utility.servlet.MetricsServlet;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Controller
@RequestMapping("/metrics")
public class MetricsController extends MetricsServlet {

  @RequestMapping()
  public void metrics(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    super.doGet(req, resp);
  }
}
