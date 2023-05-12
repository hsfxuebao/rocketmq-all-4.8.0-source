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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        // todo
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    // 遍历当前处理队列中的消息消费队列，如果当前消费者
                    //不持该消息队列的锁，则将处理队列锁的状态设置为false，暂停该消
                    //息消费队列的消息拉取与消息消费
                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        // 表示将消息队列按照Broker组织成Map<String/*brokerName*,Set<MessageQueue>>，方便下一步向
        // Broker发送锁定消息队列请求
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        // 获取broker信息
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            // 构建加锁请求
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            // 设置要加锁的消息队列
            requestBody.getMqSet().add(mq);

            try {
                // 向Broker（主节点）发送锁定消息队列，该方法会返回
                //成功被当前消费者锁定的消息消费队列
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                // 将成功锁定的消息消费队列对应的处理队列设置为锁定
                //状态，同时更新加锁时间
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        // 从处理队列表中获取broker对应的消息队列，key为broker名称，value为broker下的消息队列
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        // 遍历订阅的消息队列
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            // broker名称
            final String brokerName = entry.getKey();
            // 获取消息队列
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            // 根据broker名称获取broker信息
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                // 构建加锁请求
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                // 设置消费者组
                requestBody.setConsumerGroup(this.consumerGroup);
                // 设置ID
                requestBody.setClientId(this.mQClientFactory.getClientId());
                // 设置要加锁的消息队列
                requestBody.setMqSet(mqs);

                try {
                    // 批量进行加锁，返回加锁成功的消息队列
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    // 遍历加锁成功的队列
                    for (MessageQueue mq : lockOKMQSet) {
                        // 从处理队列表中获取对应的处理队列对象
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        // 如果不为空，设置locked为true表示加锁成功
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            // 设置加锁成功标记
                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    // 处理加锁失败的消息队列
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                // 设置加锁失败标记
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    public void doRebalance(final boolean isOrder) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    // todo 客户端负载均衡：根据主题来处理负载均衡
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            // 广播模式：不需要处理负载均衡，每个消费者都要消费，只需要更新负载信息
            case BROADCASTING: {
                // 更新负载均衡信息，这里传入的参数是mqSet，即所有队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                            consumerGroup,
                            topic,
                            mqSet,
                            mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            // todo 集群模式
            case CLUSTERING: {
                // 从主题订阅信息缓存表中获取主题的队列信息, 获取这个topic下的所有队列（默认是4个）
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                //todo 发送请求从Broker中获取该消费组内当前所有的消费者客户端ID，主题的队
                //列可能分布在多个Broker上，那么请求该发往哪个Broker呢？
                //RocketeMQ从主题的路由信息表中随机选择一个Broker
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                // 如果mqSet、cidAll任意一个为空，则忽略本次消息队列负载
                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    // 对cidAll、mqAll进行排序
                    // 这一步很重要，同一个消费组内看到的视图应保持一致，确保同一个消费队列不会被多个消费者分配
                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    // 默认是平均分配策略
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        // todo 分配算法
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                            e);
                        return;
                    }

                    //TODO:保存了当前消费者需要消费的队列
                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }


                    // todo 对比消息队列是否发生变化 更新负载均衡信息，传入参数是 allocateResultSet，即当前consumer分配到的队列
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * 对比消息队列是否发生变化，主要思路是遍历当前负载队列集
     * 合，如果队列不在新分配队列的集合中，需要将该队列停止消费并保
     * 存消费进度；遍历已分配的队列，如果队列不在队列负载表中
     * （processQueueTable），则需要创建该队列拉取任务PullRequest，
     * 然后添加到PullMessageService线程的pullRequestQueue中，
     * PullMessageService才会继续拉取任务
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        /**
         * ConcurrentMap〈MessageQueue, ProcessQueue〉
         * processQueueTable是当前消费者负载的消息队列缓存表，如果缓存表
         * 中的MessageQueue不包含在mqSet中，说明经过本次消息队列负载后，
         * 该mq被分配给其他消费者，需要暂停该消息队列消息的消费。方法是
         * 将ProccessQueue的状态设置为droped=true，该ProcessQueue中的消
         * 息将不会再被消费，调用removeUnnecessaryMessageQueue方法判断是
         * 否将MessageQueue、ProccessQueue从缓存表中移除。
         * removeUnnecessaryMessageQueue在RebalanceImple中定义为抽象方
         * 法。removeUnnecessaryMessageQueue方法主要用于持久化待移除
         * MessageQueue的消息消费进度。在推模式下，如果是集群模式并且是
         * 顺序消息消费，还需要先解锁队列
         */
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) {
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY:
                            break;
                        case CONSUME_PASSIVELY:
                            pq.setDropped(true);
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        /**
         * 遍历本次负载分配到的队列集合，如果
         * processQueueTable中没有包含该消息队列，表明这是本次新增加的消
         * 息队列，首先从内存中移除该消息队列的消费进度，然后从磁盘中读
         * 取该消息队列的消费进度，创建PullRequest对象。这里有一个关键，
         * 如果读取到的消费进度小于0，则需要校对消费进度。RocketMQ提供了
         * CONSUME_FROM_LAST_OFFSET、CONSUME_FROM_FIRST_OFFSET、
         * CONSUME_FROM_TIMESTAMP方式，在创建消费者时可以通过调用
         * DefaultMQPushConsumer#setConsumeFromWhere方法进行设置
         */
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        // 遍历队列集合
        for (MessageQueue mq : mqSet) {
            // 如果processQueueTable之前不包含当前的消息队列
            if (!this.processQueueTable.containsKey(mq)) {

                /**
                 * 经过消息队列重新负载（分配）后，分配到新的消息队列时，首
                 * 先需要尝试向Broker发起锁定该消息队列的请求，如果返回加锁成
                 * 功，则创建该消息队列的拉取任务，否则跳过，等待其他消费者释放
                 * 该消息队列的锁，然后在下一次队列重新负载时再尝试加锁
                 */
                // 如果是顺序消费，调用lock方法进行加锁，如果加锁失败不往下执行，继续处理下一个消息队列
                // todo lock
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                this.removeDirtyOffset(mq);
                ProcessQueue pq = new ProcessQueue();
                // todo PullRequest的nextOffset计算逻辑位于RebalancePushImpl#computePullFromWhere
                long nextOffset = this.computePullFromWhere(mq);
                // 如果偏移量大于等于0
                if (nextOffset >= 0) {
                    // 放入处理队列表中
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        // 如果之前不存在，构建PullRequest，之后对请求进行处理，进行消息拉取
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        //TODO:构建拉取请求对象PullRequest
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        //TODO:重点：设置初始offset值
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        // todo 将PullRequest加入PullMessageService，以便唤醒PullMessageService线程
        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
