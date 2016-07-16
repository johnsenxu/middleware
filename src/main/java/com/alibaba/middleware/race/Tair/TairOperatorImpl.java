package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.impl.*;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {
	private DefaultTairManager tairManager;
	private String masterConfigServer;
    private String slaveConfigServer;
    private String groupName;
    private int namespace;
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
    	this.masterConfigServer = masterConfigServer;
    	this.slaveConfigServer = slaveConfigServer;
    	this.groupName = groupName;
    	this.namespace = namespace;
    	// 创建config server列表
    	List<String> confServers = new ArrayList<String>();
    	confServers.add(masterConfigServer);
    	confServers.add(slaveConfigServer); // 可选

    	// 创建客户端实例
    	this.tairManager = new DefaultTairManager();
    	this.tairManager.setConfigServerList(confServers);

    	// 设置组名
    	this.tairManager.setGroupName(groupName);
    	// 初始化客户端
    	this.tairManager.init();
    	
    }

    public boolean write(Serializable key, Serializable value) {
    	ResultCode rc = tairManager.put(this.namespace, key, value);
    	if (rc.isSuccess()) {
    	    // put成功
    		return true;
    	} else if (ResultCode.VERERROR.equals(rc)){
    	    // 版本错误的处理代码
    		return false;
    	} else {
    	    // 其他失败的处理代码
    		return false;
    	}
        
    }

    public Object get(Serializable key) {
    	Result<DataEntry> result = tairManager.get(this.namespace, key);
    	if (result.isSuccess()) {
    	    DataEntry entry = result.getValue();
    	    if(entry != null) {
    	        // 数据存在
    	    	return entry;
    	    } else {
    	        // 数据不存在
    	    	return null;
    	    }
    	} else {
    	    return null;
    	}

    }

    public boolean remove(Serializable key) {
    	ResultCode rc = tairManager.delete(this.namespace, key);
    	if (rc.isSuccess()) {
    	    // 删除成功
    		return true;
    	} else {
    	    // 删除失败
    		return false;
    	}
    }

    public void close(){
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
    }
}
