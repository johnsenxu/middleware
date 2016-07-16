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

class Price{
	Double price1;
	Double price2;
	public Price(double p1, double p2){
		price1 = p1;
		price2 = p2;
	}
}

public class PayBolt implements IRichBolt {
	private static final Logger LOG = Logger.getLogger(PayBolt.class);
	protected OutputCollector _collector;
	private String prefix;
	private TreeMap<Long, Price> recordsMap;
	private TairOperatorImpl tairOperator;
    public PayBolt(String prefix) {
    	this.prefix=prefix;
    }
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector=collector;
		recordsMap = new TreeMap<Long, Price>();
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);
		writeTairThread();
	}

	
	@Override
	public void execute(Tuple input) {
		long time=(Long) input.getValueByField("orderCreateTime");
		long minuteTime = TimeUtil.getMinuteTime(time);
		double price =(Double) input.getValueByField("price");
		short platform = (Short) input.getValueByField("payPlatform");
		if (platform == 0) {
			putRecord(minuteTime, price, 0);
		}
		else {
			putRecord(minuteTime, 0, price);
		}
		_collector.ack(input);
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","price"));
	}

	private void putRecord(Long key, double price1, double price2) {
		if (recordsMap.containsKey(key)) {
			synchronized (recordsMap) {
				Price price = recordsMap.get(key);
				price.price1 += price1;
				price.price2 += price2;
				recordsMap.put(key, price);
			}
			recordsMap.notifyAll();
		} else {
			Integer currentvalue = (Integer) tairOperator.get(key);
			if (currentvalue == null) {
				synchronized (recordsMap) {
					recordsMap.put(key, new Price(price1, price2));
				}
				recordsMap.notifyAll();
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
						Price val = recordsMap.get(minuteTime);  
						if(time - minuteTime >= 3){
							StringBuilder sb=new StringBuilder(prefix+"_"+RaceConfig.TeamCode+"_");
							sb.append(TimeUtil.getMinuteTime(time));
							Double ratio = val.price2 / val.price1;
							tairOperator.write(sb.toString(), ratio);
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
