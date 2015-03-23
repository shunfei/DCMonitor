package com.sf.monitor.controllers;

import com.sf.monitor.Resources;
import com.sf.monitor.zk.ZookeeperHosts;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author: sundy
 * @since 2015-03-02.
 */
@Controller
@RequestMapping("/zk")
public class ZookeeperController {

	@RequestMapping("/hosts")
	public @ResponseBody Object hosts(){
		ZookeeperHosts zkhosts = Resources.zkHosts;
		return zkhosts.hostInfos();
	}

	@RequestMapping("/cmd")
	public @ResponseBody String cmd(String host, String cmd){
		ZookeeperHosts zkhosts = Resources.zkHosts;
		String res =  zkhosts.sendCommand(host, cmd);
		return res;
	}
}
