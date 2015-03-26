package com.sf.monitor.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.format.FormatterRegistrar;
import org.springframework.format.datetime.joda.JodaTimeFormatterRegistrar;
import org.springframework.format.support.FormattingConversionServiceFactoryBean;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: sundy
 * @since 2015-03-04.
 */
@EnableAutoConfiguration
public class DateTimeConverter {
  @Bean
  FormattingConversionServiceFactoryBean conversionService() {
    FormattingConversionServiceFactoryBean bean = new FormattingConversionServiceFactoryBean();
    Set<FormatterRegistrar> set = new HashSet<FormatterRegistrar>();
    JodaTimeFormatterRegistrar registrar = new JodaTimeFormatterRegistrar();
    registrar.setUseIsoFormat(true);
    set.add(registrar);
    bean.setFormatterRegistrars(set);
    return bean;
  }
}
