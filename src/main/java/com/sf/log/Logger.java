package com.sf.log;

import org.slf4j.LoggerFactory;

import java.util.MissingFormatArgumentException;

public class Logger {
  private final org.slf4j.Logger log;

  public Logger(String name) {
    log = LoggerFactory.getLogger(name);
  }

  public Logger(Class clazz) {
    log = LoggerFactory.getLogger(clazz);
  }

  public void trace(String message, Object... formatArgs) {
    if (log.isTraceEnabled()) {
      log.trace(safeFormat(message, formatArgs));
    }
  }

  public void trace(Throwable t, String message, Object... formatArgs) {
    if (log.isTraceEnabled()) {
      log.trace(safeFormat(message, formatArgs), t);
    }
  }

  public void debug(String message, Object... formatArgs) {
    if (log.isDebugEnabled()) {
      log.debug(safeFormat(message, formatArgs));
    }
  }

  public void debug(Throwable t, String message, Object... formatArgs) {
    if (log.isDebugEnabled()) {
      log.debug(safeFormat(message, formatArgs), t);
    }
  }

  public void info(String message, Object... formatArgs) {
    if (log.isInfoEnabled()) {
      log.info(safeFormat(message, formatArgs));
    }
  }

  public void info(Throwable t, String message, Object... formatArgs) {
    if (log.isInfoEnabled()) {
      log.info(safeFormat(message, formatArgs), t);
    }
  }

  public void warn(String message, Object... formatArgs) {
    log.warn(safeFormat(message, formatArgs));
  }

  public void warn(Throwable t, String message, Object... formatArgs) {
    log.warn(safeFormat(message, formatArgs), t);
  }

  public void error(String message, Object... formatArgs) {
    log.error(safeFormat(message, formatArgs));
  }

  public void error(Throwable t, String message, Object... formatArgs) {
    log.error(safeFormat(message, formatArgs), t);
  }

  public void wtf(String message, Object... formatArgs) {
    log.error(safeFormat("WTF?!: " + message, formatArgs), new Exception());
  }

  public void wtf(Throwable t, String message, Object... formatArgs) {
    log.error(safeFormat("WTF?!: " + message, formatArgs), t);
  }

  private String safeFormat(String message, Object... formatArgs) {
    try {
      return String.format(message, formatArgs);
    } catch (MissingFormatArgumentException e) {
      StringBuilder bob = new StringBuilder(message);
      for (Object formatArg : formatArgs) {
        bob.append("; ").append(formatArg);
      }
      return bob.toString();
    }
  }

  public boolean isTraceEnabled() {
    return log.isTraceEnabled();
  }

  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  public boolean isInfoEnabled() {
    return log.isInfoEnabled();
  }
}
