package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.common.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        int spout_Parallelism_hint = 1;
        int bolt_Parallelism_hint = 1;
        int split_Parallelism_hint = 1;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TaoBaoSpout", new MetaSpout(RaceConfig.MqTaobaoTradeTopic,RaceConfig.MetaConsumerGroup,true), spout_Parallelism_hint);
		builder.setSpout("TmallSpout", new MetaSpout(RaceConfig.MqTmallTradeTopic,RaceConfig.MetaConsumerGroup,true), spout_Parallelism_hint);
		builder.setSpout("PaymentSpout", new MetaSpout(RaceConfig.MqPayTopic,RaceConfig.MetaConsumerGroup,true), spout_Parallelism_hint);
		builder.setBolt("taobaoOrder", new DeserializationBolt("com.alibaba.middleware.race.model.OrderMessage"),
				split_Parallelism_hint).shuffleGrouping("TaoBaoSpout");
		builder.setBolt("tmallOrder", new DeserializationBolt("com.alibaba.middleware.race.model.OrderMessage"),
				split_Parallelism_hint).shuffleGrouping("TmallSpout");
		builder.setBolt("payment", new DeserializationBolt("com.alibaba.middleware.race.model.PaymentMessage"),
				split_Parallelism_hint).shuffleGrouping("PaymentSpout");
		builder.setBolt("taobaoP", new OrderBolt(RaceConfig.prex_taobao),
				1).shuffleGrouping("taobaoOrder","order");
		builder.setBolt("tmallP", new OrderBolt(RaceConfig.prex_tmall),
				1).shuffleGrouping("tmallOrder","order");
		builder.setBolt("paymentP", new PayBolt(RaceConfig.prex_ratio),
				1).shuffleGrouping("payment","payment");

        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}