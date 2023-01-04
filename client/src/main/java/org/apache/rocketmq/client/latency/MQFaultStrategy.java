/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    /**
     * 根据currentLatency本次消息发送的延迟时间，从latencyMax尾
     * 部向前找到第一个比currentLatency小的索引index，如果没有找到，
     * 则返回0。然后根据这个索引从notAvailable-Duration数组中取出对
     * 应的时间，在这个时长内，Broker将设置为不可用
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 发送延迟故障启用，默认为false
        if (this.sendLatencyFaultEnable) {
            try {
                // 获取一个index
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 选取的这个broker是可用的 直接返回
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                // 到这里 找了一圈 还是没有找到可用的broker
                // todo 选择 距离可用时间最近的
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        // todo
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     *
     * @param brokerName broker名称
     * @param currentLatency    本次消息发送的延迟时间
     * @param isolation 是否规避broker
     *                      若为true则使用30s计算broker故障规避时长，
     *                      若为false则使用本次消息发送延时计算broker故障规避时长
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 是否开启延迟故障容错
        if (this.sendLatencyFaultEnable) {
            // todo 计算不可用持续时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // todo 存储
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }


    /**
     * 计算不可用持续时间
     * @param currentLatency 当前延迟
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        // latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
        // notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};
        // 倒着遍历
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            // 如果延迟大于某个时间，就返回对应服务不可用时间，可以看出来，响应延迟100ms以下是没有问题的
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
