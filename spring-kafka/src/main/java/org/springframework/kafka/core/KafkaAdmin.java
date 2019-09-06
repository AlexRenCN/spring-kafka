/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;

/**
 * 用于在上下文创建只用创建指定主题
 * An admin that delegates to an {@link AdminClient} to create topics defined
 * in the application context.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 */
public class KafkaAdmin implements ApplicationContextAware, SmartInitializingSingleton {

	/**
	 * 默认关闭等待时间为10秒
	 * The default close timeout duration as 10 seconds.
	 */
	public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

	/**
	 * 默认操作等待时间为30秒
	 */
	private static final int DEFAULT_OPERATION_TIMEOUT = 30;

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(KafkaAdmin.class));

	private final Map<String, Object> config;

	private ApplicationContext applicationContext;

	private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private int operationTimeout = DEFAULT_OPERATION_TIMEOUT;

	/**
	 * 如果Broker不可用则异常
	 */
	private boolean fatalIfBrokerNotAvailable;

	/**
	 * 是否自动创建
	 */
	private boolean autoCreate = true;

	/**
	 * 上下文初始化中
	 */
	private boolean initializingContext;

	/**
	 * 根据配置创建实例
	 * Create an instance with an {@link AdminClient} based on the supplied
	 * configuration.
	 * @param config the configuration for the {@link AdminClient}.
	 */
	public KafkaAdmin(Map<String, Object> config) {
		this.config = new HashMap<>(config);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * 设置关闭超时时间
	 * Set the close timeout in seconds. Defaults to {@link #DEFAULT_CLOSE_TIMEOUT} seconds.
	 * @param closeTimeout the timeout.
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = Duration.ofSeconds(closeTimeout);
	}

	/**
	 * 设置操作超时时间
	 * Set the operation timeout in seconds. Defaults to {@value #DEFAULT_OPERATION_TIMEOUT} seconds.
	 * @param operationTimeout the timeout.
	 */
	public void setOperationTimeout(int operationTimeout) {
		this.operationTimeout = operationTimeout;
	}

	/**
	 * 如果想要在上下文初始化过程中无法连接到broker，就设置为true，用来检查、添加主题
	 * Set to true if you want the application context to fail to load if we are unable
	 * to connect to the broker during initialization, to check/add topics.
	 * @param fatalIfBrokerNotAvailable true to fail.
	 */
	public void setFatalIfBrokerNotAvailable(boolean fatalIfBrokerNotAvailable) {
		this.fatalIfBrokerNotAvailable = fatalIfBrokerNotAvailable;
	}

	/**
	 * 设置为false可以在上下文初始化期间不创建主题
	 * Set to false to suppress auto creation of topics during context initialization.
	 * @param autoCreate boolean flag to indicate creating topics or not during context initialization
	 * @see #initialize()
	 */
	public void setAutoCreate(boolean autoCreate) {
		this.autoCreate = autoCreate;
	}

	/**
	 * 获取管理员配置且无法修改
	 * Get an unmodifiable copy of this admin's configuration.
	 * @return the configuration map.
	 */
	public Map<String, Object> getConfig() {
		return Collections.unmodifiableMap(this.config);
	}

	@Override
	public void afterSingletonsInstantiated() {
		//标记上下文创建完成
		this.initializingContext = true;
		//如果自动创建topic
		if (this.autoCreate) {
			//检查并自动创建主题
			initialize();
		}
	}

	/**
	 * 调用这个方法来检查和添加主题
	 * 如果在初始化应用程序上下文时代理不可用，则可能需要执行此操作
	 * Call this method to check/add topics; this might be needed if the broker was not
	 * available when the application context was initialized, and
	 * {@link #setFatalIfBrokerNotAvailable(boolean) fatalIfBrokerNotAvailable} is false,
	 * or {@link #setAutoCreate(boolean) autoCreate} was set to false.
	 * @return true if successful.
	 * @see #setFatalIfBrokerNotAvailable(boolean)
	 * @see #setAutoCreate(boolean)
	 */
	public final boolean initialize() {
		//统计上下文中需要创建的主题
		Collection<NewTopic> newTopics = this.applicationContext.getBeansOfType(NewTopic.class, false, false).values();
		//如果有需要创建的主题
		if (newTopics.size() > 0) {
			AdminClient adminClient = null;
			try {
				//通过配置创建AdminClient
				adminClient = AdminClient.create(this.config);
			}
			catch (Exception e) {
				//上下文没有初始化  或者   如果Broker不可用则异常 就抛出异常
				if (!this.initializingContext || this.fatalIfBrokerNotAvailable) {
					throw new IllegalStateException("Could not create admin", e);
				}
				else {
					LOGGER.error(e, "Could not create admin");
				}
			}
			//如果创建了AdminClient
			if (adminClient != null) {
				try {
					//检查或创建主题
					addTopicsIfNeeded(adminClient, newTopics);
					return true;
				}
				catch (Exception e) {
					if (!this.initializingContext || this.fatalIfBrokerNotAvailable) {
						throw new IllegalStateException("Could not configure topics", e);
					}
					else {
						LOGGER.error(e, "Could not configure topics");
					}
				}
				finally {
					//上下文初始化中为false
					this.initializingContext = false;
					//关闭AdminClient
					adminClient.close(this.closeTimeout);
				}
			}
		}
		//上下文初始化中为false
		this.initializingContext = false;
		return false;
	}

	private void addTopicsIfNeeded(AdminClient adminClient, Collection<NewTopic> topics) {
		//需要创建的主题数>0时
		if (topics.size() > 0) {
			Map<String, NewTopic> topicNameToTopic = new HashMap<>();
			//检查topicNameToTopic为空项
			topics.forEach(t -> topicNameToTopic.compute(t.name(), (k, v) -> t));
			//获取主题信息
			DescribeTopicsResult topicInfo = adminClient
					//使用默认选项描述主题
					.describeTopics(topics.stream()
							.map(NewTopic::name)
							.collect(Collectors.toList()));
			List<NewTopic> topicsToAdd = new ArrayList<>();
			//通过检查偏移量来确认主题是需要新增还是修改
			Map<String, NewPartitions> topicsToModify = checkPartitions(topicNameToTopic, topicInfo, topicsToAdd);
			if (topicsToAdd.size() > 0) {
				//添加主题
				addTopics(adminClient, topicsToAdd);
			}
			if (topicsToModify.size() > 0) {
				//修改主题
				modifyTopics(adminClient, topicsToModify);
			}
		}
	}

	private Map<String, NewPartitions> checkPartitions(Map<String, NewTopic> topicNameToTopic,
			DescribeTopicsResult topicInfo, List<NewTopic> topicsToAdd) {

		Map<String, NewPartitions> topicsToModify = new HashMap<>();
		topicInfo.values().forEach((n, f) -> {
			//获取需要检查或创建的主题
			NewTopic topic = topicNameToTopic.get(n);
			try {
				//获取主题的详细信息
				TopicDescription topicDescription = f.get(this.operationTimeout, TimeUnit.SECONDS);
				//分区数小于实际的分区数
				if (topic.numPartitions() < topicDescription.partitions().size()) {
					LOGGER.info(() -> String.format(
						"Topic '%s' exists but has a different partition count: %d not %d", n,
						topicDescription.partitions().size(), topic.numPartitions()));
				}
				//分区数大于实际的分区数
				else if (topic.numPartitions() > topicDescription.partitions().size()) {
					LOGGER.info(() -> String.format(
						"Topic '%s' exists but has a different partition count: %d not %d, increasing "
						+ "if the broker supports it", n,
						topicDescription.partitions().size(), topic.numPartitions()));
					//需要对实际的分区数进行修改
					topicsToModify.put(n, NewPartitions.increaseTo(topic.numPartitions()));
				}
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (TimeoutException e) {
				throw new KafkaException("Timed out waiting to get existing topics", e);
			}
			catch (@SuppressWarnings("unused") ExecutionException e) {
				//获取不到主题，则需要添加主题
				topicsToAdd.add(topic);
			}
		});
		return topicsToModify;
	}

	private void addTopics(AdminClient adminClient, List<NewTopic> topicsToAdd) {
		//异步添加主题
		CreateTopicsResult topicResults = adminClient.createTopics(topicsToAdd);
		try {
			//获取添加主题的结果
			topicResults.all().get(this.operationTimeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error(e, "Interrupted while waiting for topic creation results");
		}
		catch (TimeoutException e) {
			throw new KafkaException("Timed out waiting for create topics results", e);
		}
		catch (ExecutionException e) {
			//如果是主题已经被创建的异常，则忽略，因为可能是集群中其他应用创建了这个主题
			if (e.getCause() instanceof TopicExistsException) { // Possible race with another app instance
				LOGGER.debug(e.getCause(), "Failed to create topics");
			}
			else {
				LOGGER.error(e.getCause(), "Failed to create topics");
				throw new KafkaException("Failed to create topics", e.getCause()); // NOSONAR
			}
		}
	}

	private void modifyTopics(AdminClient adminClient, Map<String, NewPartitions> topicsToModify) {
		//异步修改主题
		CreatePartitionsResult partitionsResult = adminClient.createPartitions(topicsToModify);
		try {
			//获取结果
			partitionsResult.all().get(this.operationTimeout, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error(e, "Interrupted while waiting for partition creation results");
		}
		catch (TimeoutException e) {
			throw new KafkaException("Timed out waiting for create partitions results", e);
		}
		catch (ExecutionException e) {
			//如果分区数无效，则忽略，因为可能是集群中其他应用创建了这个主题
			if (e.getCause() instanceof InvalidPartitionsException) { // Possible race with another app instance
				LOGGER.debug(e.getCause(), "Failed to create partitions");
			}
			else {
				LOGGER.error(e.getCause(), "Failed to create partitions");
				if (!(e.getCause() instanceof UnsupportedVersionException)) {
					throw new KafkaException("Failed to create partitions", e.getCause()); // NOSONAR
				}
			}
		}
	}

}
