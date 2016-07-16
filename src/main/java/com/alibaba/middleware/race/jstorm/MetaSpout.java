package com.alibaba.middleware.race.jstorm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.rocketmq.MQClientConfig;
import com.alibaba.middleware.race.rocketmq.DefaultMQConsumerFactory;
import com.alibaba.middleware.race.rocketmq.MetaTuple;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MetaSpout implements IRichSpout, IAckValueSpout, IFailValueSpout, MessageListenerConcurrently {
	/**  */
	private static final long serialVersionUID = 8476906628618859716L;
	private static final Logger LOG = Logger.getLogger(MetaSpout.class);

	protected MQClientConfig mqClientConfig;
	protected SpoutOutputCollector collector;
	protected transient DefaultMQPushConsumer consumer;

	protected Map<String, String> conf;
	protected String id;
	protected boolean flowControl;
	protected boolean autoAck;

	protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;

	// protected transient MetricClient metricClient;
	// protected transient JStormHistogram waithHistogram;
	// protected transient JStormHistogram processHistogram;
	protected String topic;
	protected String group;
	private boolean isStatEnable;
	private long sendingCount;
	private long startTime;
	private int sendNumPerNexttuple;

	public MetaSpout() {

	}

	public MetaSpout(String topic, String consumerGroup, boolean flowControl) {
		this.topic = topic;
		this.group = consumerGroup;
		this.flowControl = flowControl;
		this.conf=new HashMap<String, String>();
//		this._conf.put(MQClientConfig.MQ_NAMESERVER, "192.168.1.149:9876");
		this.conf.put(MQClientConfig.MQ_TOPIC, this.topic);
		this.conf.put(MQClientConfig.MQ_CONSUMER_GROUP, this.group);
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
		this.sendingQueue = new LinkedBlockingDeque<MetaTuple>();
		this.sendingCount = 0;
		this.startTime = System.currentTimeMillis();
		this.sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
		this.isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
		this.flowControl = JStormUtils.parseBoolean(conf.get(MQClientConfig.MQ_SPOUT_FLOW_CONTROL), true);
		this.autoAck = JStormUtils.parseBoolean(conf.get(MQClientConfig.MQ_SPOUT_AUTO_ACK), false);

		StringBuilder sb = new StringBuilder();
		sb.append("Begin to init MetaSpout:").append(id);
		sb.append(", flowControl:").append(flowControl);
		sb.append(", autoAck:").append(autoAck);
		LOG.info(sb.toString());

		// initMetricClient(context);

		mqClientConfig = MQClientConfig.mkInstance(this.conf);

		try {
			consumer = DefaultMQConsumerFactory.mkInstance(mqClientConfig, this);
		} catch (Exception e) {
			LOG.error("Failed to create Meta Consumer ", e);
			throw new RuntimeException("Failed to create MetaConsumer" + id, e);
		}

		if (consumer == null) {
			LOG.warn(id + " already exist consumer in current worker, don't need to fetch data ");

			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							break;
						}

						StringBuilder sb = new StringBuilder();
						sb.append("Only on meta consumer can be run on one process,");
						sb.append(
								" but there are mutliple spout consumes with the same topic@groupid meta, so the second one ");
						sb.append(id).append(" do nothing ");
						LOG.info(sb.toString());
					}
				}
			}).start();
		}

		LOG.info("Successfully init " + id);
	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.shutdown();
		}

	}

	@Override
	public void activate() {
		if (consumer != null) {
			consumer.resume();
		}

	}

	@Override
	public void deactivate() {
		if (consumer != null) {
			consumer.suspend();
		}
	}

	public void sendTuple(MetaTuple metaTuple) {
		metaTuple.updateEmitMs();
		collector.emit(new Values(metaTuple), metaTuple.getCreateMs());
	}

	@Override
	public void nextTuple() {
		int n = sendNumPerNexttuple;
		while (--n >= 0) {
			Utils.sleep(10);
			MetaTuple metaTuple = null;
			try {
				metaTuple = sendingQueue.take();
			} catch (InterruptedException e) {
              
			}
			if (metaTuple == null) {
				return;
			}

			sendTuple(metaTuple);
		}

		updateSendTps();
	}

	@Override
	@Deprecated
	public void ack(Object msgId) {
		LOG.warn("Shouldn't go this function");
	}

	@Override
	@Deprecated
	public void fail(Object msgId) {
		LOG.warn("Shouldn't go this function");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MetaTuple"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		MetaTuple metaTuple = (MetaTuple) values.get(0);
		AtomicInteger failTimes = metaTuple.getFailureTimes();

		int failNum = failTimes.incrementAndGet();
		if (failNum > mqClientConfig.getMaxFailTimes()) {
			LOG.warn("Message " + metaTuple.getMq() + " fail times " + failNum);
			metaTuple.done();
			return;
		}

		if (flowControl) {
			sendingQueue.offer(metaTuple);
		} else {
			sendTuple(metaTuple);
		}
	}


	@Override
	public void ack(Object msgId, List<Object> values) {
		MetaTuple metaTuple = (MetaTuple) values.get(0);
		metaTuple.done();
	}

	public DefaultMQPushConsumer getConsumer() {
		return consumer;
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		try {
			MetaTuple metaTuple = new MetaTuple(msgs, context.getMessageQueue());

			if (flowControl) {
				sendingQueue.offer(metaTuple);
			} else {
				sendTuple(metaTuple);
			}

			if (autoAck) {
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			} else {
				metaTuple.waitFinish();
				if (metaTuple.isSuccess() == true) {
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				} else {
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
			}

		} catch (Exception e) {
			LOG.error("Failed to emit " + id, e);
			return ConsumeConcurrentlyStatus.RECONSUME_LATER;
		}
	}

	private void updateSendTps() {
		if (!isStatEnable)
			return;

		sendingCount++;
		long now = System.currentTimeMillis();
		long interval = now - startTime;
		if (interval > 60 * 1000) {
			LOG.info("Sending tps of last one minute is " + (sendingCount * sendNumPerNexttuple * 1000) / interval);
			startTime = now;
			sendingCount = 0;
		}
	}

}