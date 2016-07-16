package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.MetaTuple;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DeserializationBolt implements IRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6948872390457493673L;
	protected OutputCollector collector;
	private static final Logger LOG = Logger.getLogger(DeserializationBolt.class);
	private String clazz;
	private String type;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
   
	public DeserializationBolt(String clazz) {
		this.clazz = clazz;
	}

	@Override
	public void execute(Tuple input) {
		MetaTuple tuple = (MetaTuple) input.getValue(0);
		List<MessageExt> msgs=tuple.getMsgList();
		for(MessageExt msg : msgs)
		{
			try {
				Object obj=RaceUtils.readKryoObject(Class.forName(clazz), msg.getBody());
			    if(obj instanceof OrderMessage) {
			    	OrderMessage orderMsg=(OrderMessage)obj;
			    	long createTime=orderMsg.getCreateTime();
			    	
			    	double price=orderMsg.getTotalPrice();
			    	collector.emit("order",input,new Values(createTime,price));
			    	
			    }
			    if(obj instanceof PaymentMessage) {
			    	PaymentMessage payMsg=(PaymentMessage)obj;
			    	long createTime=payMsg.getCreateTime();
			    	double amount=payMsg.getPayAmount();
			    	short platform=payMsg.getPayPlatform();
			    	collector.emit("payment",input,new Values(createTime,amount,platform));
			    }
			   
			} catch (ClassNotFoundException e) {
				Log.info("can not find "+clazz+"Deserialize Object faile ");
				e.printStackTrace();
			}
		}
		tuple.done();
		try {
			LOG.info("Messages:" + tuple);

		} catch (Exception e) {
			collector.fail(input);
			return;
			// throw new FailedException(e);
		}
		collector.ack(input);

	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("order", new Fields("orderCreateTime","price"));
		declarer.declareStream("payment",new Fields("paymentCreateTime","amount","payPlatform"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
