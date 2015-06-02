package com.sf.monitor.config;

import org.apache.velocity.app.Velocity;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.bind.RelaxedPropertyResolver;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.view.velocity.VelocityConfigurer;
import org.springframework.web.servlet.view.velocity.VelocityViewResolver;

import javax.servlet.Servlet;
import java.util.Properties;

/**
 * @author: sundy
 * @since 2015-03-04.
 */
@Configuration
@ConditionalOnClass({Servlet.class})
@AutoConfigureAfter(WebMvcAutoConfiguration.class)
class VelocityConfiguration implements EnvironmentAware {

  private RelaxedPropertyResolver environment;

  @Override
  public void setEnvironment(Environment environment) {
    this.environment = new RelaxedPropertyResolver(environment, "spring.velocity.");
  }

  @Bean
  VelocityConfigurer velocityConfig() {
    return new VelocityConfigurer();
  }

  @Bean
  VelocityViewResolver velocityViewResolver() {
    VelocityViewResolver resolver = new VelocityViewResolver();
    resolver.setSuffix(this.environment.getProperty("suffix", ".vm"));
    resolver.setPrefix(this.environment.getProperty("prefix", "/public/"));
    // Needs to come before any fallback resolver (e.g. a
    // InternalResourceViewResolver)
    resolver.setOrder(Ordered.LOWEST_PRECEDENCE - 20);
    Properties p = new Properties();
    p.put(Velocity.FILE_RESOURCE_LOADER_PATH, "/public/");
    p.put("input.encoding", "utf-8");
    p.put("output.encoding", "utf-8");
    resolver.setAttributes(p);
    resolver.setContentType("text/html;charset=utf-8");
    return resolver;
  }
}