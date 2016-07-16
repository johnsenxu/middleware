package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

class Count{
	public long payMo;
	public long payPc;
	public long orderTM;
	public long orderTB;
	public Count() {
		// TODO Auto-generated constructor stub
		this.payMo = 0;
		this.payPc = 0;
		this.orderTB = 0;
		this.orderTM = 0;
	}
}

public class MQBolt implements IRichBolt {
    OutputCollector collector;
    TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,RaceConfig.TairGroup, RaceConfig.TairNamespace);
    TreeMap<Long, Count> treeMap = new TreeMap<Long, Count>();
	private String teamcode;
    @Override
    public void execute(Tuple tuple) {
    	Long minuteTime = tuple.getLong(0);
    	String type = tuple.getString(1);
    	Count count = treeMap.get(minuteTime);
        if (type.equals("TMOrder")){
        	count.orderTM += 1;
        }
        if (type.equals("TBOrder")){
        	count.orderTB += 1;
        }
        if (type.equals("Pay")){
        	Short payPlatform = tuple.getShort(2);
        	Short tShort = 0;
        	if (payPlatform.equals(tShort)){
        		count.payPc += 1;
        	}
        }
        treeMap.put(minuteTime, count);
        if (treeMap.size() > 5) {
        	Long first = treeMap.firstKey();
    		Count val = treeMap.get(first);
    		
    		tairOperator.write(RaceConfig.prex_tmall + this.teamcode + "_ " + minuteTime.toString(), val.orderTM);
    		tairOperator.write(RaceConfig.prex_taobao + this.teamcode + "_ " +  minuteTime.toString(), val.orderTB);
    		double t = val.payMo * 1.0 / val.payPc;
    		double ratio = Math.round( t * 100 ) / 100.0;
    		tairOperator.write(RaceConfig.prex_ratio + this.teamcode + "_ " +  minuteTime.toString(), ratio);
    		treeMap.remove(first);
			
		}
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    	for (Iterator<Long> it = treeMap.keySet().iterator(); it.hasNext();) {// 遍例集合  
			Long minuteTime = it.next();
			Count val = treeMap.get(minuteTime);  
			tairOperator.write(RaceConfig.prex_tmall  + this.teamcode + "_ " + minuteTime.toString(), val.orderTM);
    		tairOperator.write(RaceConfig.prex_taobao + this.teamcode + "_ " +  minuteTime.toString(), val.orderTB);
    		double t = val.payMo * 1.0 / val.payPc;
    		double ratio = Math.round( t * 100 ) / 100.0;
    		tairOperator.write(RaceConfig.prex_ratio + this.teamcode + "_ " +  minuteTime.toString(), ratio);  
		}
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}