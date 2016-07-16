package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.TimeUtil;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.TimeUtil;

public class OrderBolt implements IRichBolt {
	private static final Logger LOG = Logger.getLogger(OrderBolt.class);
	protected OutputCollector _collector;
	private String prefix;
	private TreeMap<Long, Double> recordsMap;
	private TairOperatorImpl tairOperator;
    public OrderBolt(String prefix) {
    	this.prefix=prefix;
    }
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector=collector;
		recordsMap = new TreeMap<Long, Double>();
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);
		writeTairThread();
	}

	
	@Override
	public void execute(Tuple input) {
		long time=(Long) input.getValueByField("orderCreateTime");
		long minuteTime = TimeUtil.getMinuteTime(time);
		double price =(Double) input.getValueByField("price");
		StringBuilder sb=new StringBuilder(prefix+"_"+RaceConfig.TeamCode+"_");
		sb.append(TimeUtil.getMinuteTime(time));
		putRecord(minuteTime, price);
		_collector.emit(input, new Values(sb.toString(),price));
		_collector.ack(input);
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","price"));
	}

	private void putRecord(Long key, double price) {
		if (recordsMap.containsKey(key)) {
			synchronized (recordsMap) {
				recordsMap.put(key, price + recordsMap.get(key));
			}
			recordsMap.notifyAll();
		} else {
			Integer currentvalue = (Integer) tairOperator.get(key);
			if (currentvalue == null) {
				synchronized (recordsMap) {
					recordsMap.put(key, price);
				}
				recordsMap.notifyAll();
			} else {
				tairOperator.write(key, price + currentvalue);
			}
			
		}
	}
	private void writeTairThread() {
		Thread update = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					long time = TimeUtil.getMinuteTime(System.currentTimeMillis());
					for (Iterator<Long> it = recordsMap.keySet().iterator(); it.hasNext();) {// 遍例集合  
						Long minuteTime = it.next();
						Double val = recordsMap.get(minuteTime);  
						if(time - minuteTime >= 3){
							StringBuilder sb=new StringBuilder(prefix+"_"+RaceConfig.TeamCode+"_");
							sb.append(TimeUtil.getMinuteTime(time));
							tairOperator.write(sb.toString(), val);
							recordsMap.remove(minuteTime);
						}
					}
				}

			}
		});
		update.start();
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
