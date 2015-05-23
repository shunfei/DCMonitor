package com.sf.monitor.zk;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.log.Logger;
import com.sf.monitor.CommonFetcher;
import com.sf.monitor.InfoFetcher;
import com.sf.monitor.Resources;
import org.joda.time.Period;

public class ZookeeperInfoFetcher extends CommonFetcher {
  private static final Logger log = new Logger(ZookeeperInfoFetcher.class);

  @JsonProperty
  public String fetchPeriod;

  private ZookeeperHosts zkHosts;
  private Thread fetchThread;
  private boolean stopped;
  private long fetchCount;

  @Override
  public String type() {
    return InfoFetcher.Zookeeper;
  }

  @Override
  public void start() throws Exception {
    long t = Period.parse(fetchPeriod).toStandardDuration().getMillis();
    final long period = t <= 1000 ? 1000 : t;
		zkHosts = Resources.zkHosts;
    fetchThread = new Thread() {
      @Override
      public void run() {
        while (!stopped && !Thread.interrupted()) {
          try {
            fetchCount++;
            log.info("zookeeper fetch [%s] times", fetchCount);

            zkHosts.pulse();

            Thread.sleep(period);
          } catch (Exception e) {
            // InterruptedException is normal case, do nothing.
            if (!(e instanceof InterruptedException)) {
              log.error(e, "%s loop error", ZookeeperInfoFetcher.class.getSimpleName());
            } else {
              log.debug(e, "interuppted");
            }
          }
        }

        log.info("%s fetch thread stopped!", ZookeeperInfoFetcher.class.getSimpleName());
      }
    };
    fetchThread.start();

    log.info("%s started!", ZookeeperInfoFetcher.class.getSimpleName());
  }

  @Override
  public void stop() {
    stopped = true;
    if (fetchThread != null) {
      fetchThread.interrupt();
    }
  }
}
