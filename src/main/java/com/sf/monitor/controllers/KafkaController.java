package com.sf.monitor.controllers;

import com.sf.monitor.Resources;
import com.sf.monitor.kafka.KafkaInfos;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: sundy
 * @since 2015-03-02.
 */
@Controller
@RequestMapping("/kafka")
public class KafkaController {

	@Value("${kafka.query.time.offset}")
	private  String timeOffset;

	@Value("${kafka.query.flushInterval}")
	private String period;

	@RequestMapping("/hosts")
	public
	@ResponseBody
	Object hosts() {
		return Resources.kafkaInfos.getCluster();
	}

	@RequestMapping("/active")
	public
	@ResponseBody
	Object active() {
		KafkaInfos.ActiveTopics topics = Resources.kafkaInfos.getActiveTopicMap();
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (String s : topics.topicToConsumer.keySet()) {
			Set<String> ss = topics.topicToConsumer.get(s);
			Map<String, String> tmp = new LinkedHashMap<String, String>();
			for (String consumer : ss) {
				tmp.put("Topic", s);
				tmp.put("Consumer", consumer);
				list.add(tmp);
			}
		}
		return list;
	}

	@RequestMapping("/consumer")
	public
	@ResponseBody
	Object consumer() {
		List<String> list = Resources.kafkaInfos.getGroups();
		List<Map<String, String>> listMap = new ArrayList<Map<String, String>>();
		for (String s : list) {
			Map<String, String> tmp = new HashMap<String, String>();
			tmp.put("Consumers", s);
			listMap.add(tmp);
		}
		return listMap;
	}

	@RequestMapping("/topic")
	public
	@ResponseBody
	Object topic() {
		List<String> list = Resources.kafkaInfos.getTopics();
		List<Map<String, String>> listMap = new ArrayList<Map<String, String>>();
		for (String s : list) {
			Map<String, String> tmp = new HashMap<String, String>();
			tmp.put("Topics", s);
			listMap.add(tmp);
		}
		return listMap;
	}

	@RequestMapping("/topic/{topic}")
	public String topicDetailHtml(@PathVariable String topic, String consumer, Map<String, Object> mp) {
		mp.put("topic", topic);
		mp.put("consumer", consumer);
		mp.put("period", period);
		return "topic_consumer";
	}

	@RequestMapping("/detail")
	public
	@ResponseBody
	Object topicDetail(String topic, String consumer, String from, String to) {
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		DateTime fromDate = null;
		try {
			fromDate = DateTime.parse(from, formatter);
		} catch (Exception e) {
			fromDate = new DateTime();
		}
		DateTime toDate = null;
		try {
			toDate = DateTime.parse(to, formatter);
		} catch (Exception e) {
			toDate = new DateTime();
		}
		if (from == null || to.equals(from)) {
			fromDate = toDate.minus(new Period(timeOffset));
		}
		return Resources.kafkaInfos.getTrendConsumeInfos(consumer, topic, fromDate, toDate);
	}

	@RequestMapping("/consumerInfo")
	public @ResponseBody Object consumerInfos(final String topic, String consumer){
		Object o = Resources.kafkaInfos.getConsumerInfos(consumer,	new HashSet<String>(){{add(topic);}});
		return o;
	}
}

