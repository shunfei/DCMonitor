package com.sf.monitor.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.influxdb.dto.Point;
import com.sf.log.Logger;
import com.sf.monitor.CommonFetcher;
import com.sf.monitor.InfoFetcher;
import org.joda.time.Period;

import java.util.List;

public class KafkaInfoFetcher extends CommonFetcher {
  private static final Logger log = new Logger(KafkaInfoFetcher.class);

  @JsonProperty
  public String fetchPeriod;

  private Thread fetchThread;
  private boolean stopped;
  private long fetchCount;

  @Override
  public String type() {
    return InfoFetcher.Kafka;
  }


  @Override
  public void start() throws Exception {
    long t = Period.parse(fetchPeriod).toStandardDuration().getMillis();
    final long period = t <= 1000 ? 1000 : t;

    fetchThread = new Thread() {
      @Override
      public void run() {
        while (!stopped && !Thread.interrupted()) {
          try {
            fetchCount++;
            log.info("kafka fetch [%s] times", fetchCount);

            List<Point> series = KafkaStats.fetchTrendInfos();
            saveMetrics(series);

            Thread.sleep(period);
          } catch (Exception e) {
            // InterruptedException is normal case, do nothing.
            if (!(e instanceof InterruptedException)) {
              log.error(e, "%s loop error", KafkaInfoFetcher.class.getSimpleName());
            } else {
              log.debug(e, "interuppted");
            }
          }
        }

        log.info("%s fetch thread stopped!", KafkaInfoFetcher.class.getSimpleName());
      }
    };
    fetchThread.start();

    log.info("%s started!", KafkaInfoFetcher.class.getSimpleName());
  }

  @Override
  public void stop() {
    stopped = true;
    if (fetchThread != null) {
      fetchThread.interrupt();
    }
  }

}
