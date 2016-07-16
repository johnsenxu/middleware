package com.alibaba.middleware.race.rocketmq;


import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeFormat;

/**
 * Meta Spout Setting
 * 
 * All needed configs must prepare before submit topopoly
 * 
 * @author kevin
 */
public class MQClientConfig implements Serializable {

	private static final long serialVersionUID = 4157424979688593280L;

	public static final String MQ_TOPIC = "mq.topic";
	public static final String MQ_CONSUMER_GROUP = "mq.consumer.group";
	public static final String MQ_SUBEXPRESS = "mq.subexpress";
	public static final String MQ_NAMESERVER = "mq.nameserver";
	//pull interval(ms) from meta server 
	public static final String MQ_PULL_INTERVAL = "mq.pull.interval.ms";
	// max fail times
	public static final String MQ_MAX_FAIL_TIMES = "mq.max.fail.times";
	// meta client internal queue size
	public static final String MQ_INTERNAL_QUEUE_SIZE = "mq.internal.queue.size";
	// spout send one batch size
	public static final String MQ_BATCH_SEND_MSG_SIZE = "mq.batch.send.msg.size";
	// meta client pull batch size from meta server
	public static final String MQ_BATCH_PULL_MSG_SIZE = "mq.batch.pull.msg.size";
	// meta client pull thread num
	public static final String MQ_PULL_THREAD_NUM = "mq.pull.thread.num";
	// meta message automatically ack
	public static final String MQ_SPOUT_AUTO_ACK = "mq.spout.auto.ack";
	// enable meta spout flow control
	public static final String MQ_SPOUT_FLOW_CONTROL= "mq.spout.flow.control";
	
	// format is "yyyyMMddHHmmss"
	// set the meta client offset from this timestamp
	public static final String MQ_CONSUMER_START_TIMESTAMP = "mq.consumer.start.timestamp";
	public static final String MQ_EXTRA_PROPERTIES = "mq.extra.properties";


	private final String consumerGroup;

	/**
	 * Alipay need set nameServer, taobao don't need set this field
	 */
	private final String nameServer;

	private final String topic;

	private final String subExpress;

	/**
	 * The max allowed failures for one single message, skip the failure message
	 * if excesses
	 * 
	 * -1 means try again until success
	 */
	private int maxFailTimes = DEFAULT_FAIL_TIME;
	public static final int DEFAULT_FAIL_TIME = 5;

	/**
	 * Local messages threshold, trigger flow control if excesses
	 * 
	 */
	private int queueSize = DEFAULT_QUEUE_SIZE;
	public static final int DEFAULT_QUEUE_SIZE = 256;

	/**
	 * fetch messages size from local queue
	 * it is also sending batch size
	 * 
	 */
	private int sendBatchSize = DEFAULT_BATCH_MSG_NUM;
	public static final int DEFAULT_BATCH_MSG_NUM = 32;

	/**
	 * pull message size from meta server 
	 * 
	 */
	private int pullBatchSize = DEFAULT_BATCH_MSG_NUM;

	/**
	 * pull interval(ms) from server for every batch
	 * 
	 */
	private long pullInterval = 0;

	/**
	 * pull threads num
	 */
	private int pullThreadNum = DEFAULT_PULL_THREAD_NUM;
	public static int DEFAULT_PULL_THREAD_NUM = 4;

	/**
	 * Consumer start time Null means start from the last consumption
	 * time(CONSUME_FROM_LAST_OFFSET)
	 * 
	 */
	private Date startTimeStamp;

	private Properties peroperties;

	protected MQClientConfig(String consumerGroup, String nameServer,
			String topic, String subExpress) {
		this.consumerGroup = consumerGroup;
		this.nameServer = nameServer;
		this.topic = topic;
		this.subExpress = subExpress;
	}
	
	public MQClientConfig(Map conf) {
		topic = (String) conf.get(MQ_TOPIC);
		consumerGroup = (String) conf.get(MQ_CONSUMER_GROUP);
		subExpress = (String) conf.get(MQ_SUBEXPRESS);
		if (StringUtils.isBlank((String) conf.get(MQ_NAMESERVER)) == false) {
			nameServer = (String) conf.get(MQ_NAMESERVER);
		}else {
			nameServer = null;
		}
		
		maxFailTimes = JStormUtils.parseInt(conf.get(MQ_MAX_FAIL_TIMES), 
				DEFAULT_FAIL_TIME);
		
		queueSize = JStormUtils.parseInt(conf.get(MQ_INTERNAL_QUEUE_SIZE), 
				DEFAULT_QUEUE_SIZE);
		
		sendBatchSize = JStormUtils.parseInt(conf.get(MQ_BATCH_SEND_MSG_SIZE),
				DEFAULT_BATCH_MSG_NUM);
		
		pullBatchSize = JStormUtils.parseInt(conf.get(MQ_BATCH_PULL_MSG_SIZE),
				DEFAULT_BATCH_MSG_NUM);
		
		pullInterval = JStormUtils.parseInt(conf.get(MQ_PULL_INTERVAL), 0);
		
		pullThreadNum = JStormUtils.parseInt(conf.get(MQ_PULL_THREAD_NUM), 
				DEFAULT_PULL_THREAD_NUM);
		
		String ts = (String)conf.get(MQ_CONSUMER_START_TIMESTAMP);
		if (ts != null) {
			Date date = null;
			try {
				date = TimeFormat.getSecond(ts);
			}catch(Exception e) {
				
			}
			
			if (date != null) {
				startTimeStamp = date;
			}
		}
		
		Object prop = conf.get(MQ_EXTRA_PROPERTIES);
		if (prop != null && prop instanceof Properties) {
			peroperties = (Properties)prop;
		}
	}

	public static MQClientConfig mkInstance(Map conf) {

		return new MQClientConfig(conf);
	}

	

	public int getMaxFailTimes() {
		return maxFailTimes;
	}

	public void setMaxFailTimes(int maxFailTimes) {
		this.maxFailTimes = maxFailTimes;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public int getSendBatchSize() {
		return sendBatchSize;
	}

	public void setSendBatchSize(int sendBatchSize) {
		this.sendBatchSize = sendBatchSize;
	}

	public int getPullBatchSize() {
		return pullBatchSize;
	}

	public void setPullBatchSize(int pullBatchSize) {
		this.pullBatchSize = pullBatchSize;
	}

	public long getPullInterval() {
		return pullInterval;
	}

	public void setPullInterval(long pullInterval) {
		this.pullInterval = pullInterval;
	}

	public int getPullThreadNum() {
		return pullThreadNum;
	}

	public void setPullThreadNum(int pullThreadNum) {
		this.pullThreadNum = pullThreadNum;
	}

	public Date getStartTimeStamp() {
		return startTimeStamp;
	}

	public void setStartTimeStamp(Date startTimeStamp) {
		this.startTimeStamp = startTimeStamp;
	}

	public Properties getPeroperties() {
		return peroperties;
	}

	public void setPeroperties(Properties peroperties) {
		this.peroperties = peroperties;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public String getNameServer() {
		return nameServer;
	}

	public String getTopic() {
		return topic;
	}

	public String getSubExpress() {
		return subExpress;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
