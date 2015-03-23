package com.sf.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * @author: sundy
 * @since 2015-03-03.
 */
@EnableAutoConfiguration
@ComponentScan
@Configuration
public class DCMonitor extends SpringBootServletInitializer {

  private static final Logger logger = LoggerFactory.getLogger(DCMonitor.class);

  @Override
  protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
    return application.sources(DCMonitor.class);
  }

  public static void main(String[] args) {
    preare();
    SpringApplication.run(DCMonitor.class);
  }

  private static void preare(){
    Config.init("config");
    Resources.init();

    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        @Override
        public void run() {
          for (InfoFetcher fetcher : Config.config.fetcherList()) {
            fetcher.stop();
          }
          Resources.close();
          try {
            // Wait for other threads to exit. 2 seconds should be enough.
            TimeUnit.SECONDS.sleep(2);
            logger.info("DCMonitor system exist!");
          } catch (InterruptedException e) {
          }
        }
      }
    );

    for (InfoFetcher fetcher : Config.config.fetcherList()) {
      try {
        fetcher.start();
      } catch (Exception e) {
        logger.error(e + "start %s failed!"+ fetcher.type());
        System.exit(-1);
      }
    }
  }

}
