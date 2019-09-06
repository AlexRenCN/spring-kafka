/*
 * Copyright 2015-2019 the original author or authors.
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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;


/**
 * A template for executing high-level operations.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Biju Kunjummen
 * @author Endika Guti?rrez
 */
public class KafkaTemplate<K, V> implements KafkaOperations<K, V> {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass())); //NOSONAR

	private final ProducerFactory<K, V> producerFactory;

	/**
	 * 每次发送后刷新
	 */
	private final boolean autoFlush;

	/**
	 * 是否支持事务
	 */
	private final boolean transactional;

	private final ThreadLocal<Producer<K, V>> producers = new ThreadLocal<>();

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private volatile String defaultTopic;

	private volatile ProducerListener<K, V> producerListener = new LoggingProducerListener<K, V>();

	/**
	 * 事务ID前缀
	 */
	private String transactionIdPrefix;

	private Duration closeTimeout = ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT;

	/**
	 * 通过生产者工厂创建实例，自动刷新默认为false
	 * Create an instance using the supplied producer factory and autoFlush false.
	 * @param producerFactory the producer factory.
	 */
	public KafkaTemplate(ProducerFactory<K, V> producerFactory) {
		this(producerFactory, false);
	}

	/**
	 * 通过生产者工厂和指定的自动刷新配置创建实例
	 * Create an instance using the supplied producer factory and autoFlush setting.
	 * <p>
	 * Set autoFlush to {@code true} if you have configured the producer's
	 * {@code linger.ms} to a non-default value and wish send operations on this template
	 * to occur immediately, regardless of that setting, or if you wish to block until the
	 * broker has acknowledged receipt according to the producer's {@code acks} property.
	 * @param producerFactory the producer factory.
	 * @param autoFlush true to flush after each send.
	 * @see Producer#flush()
	 */
	public KafkaTemplate(ProducerFactory<K, V> producerFactory, boolean autoFlush) {
		this.producerFactory = producerFactory;
		this.autoFlush = autoFlush;
		this.transactional = producerFactory.transactionCapable();
	}

	/**
	 * 未提供主题的消息发送到默认主题
	 * The default topic for send methods where a topic is not
	 * provided.
	 * @return the topic.
	 */
	public String getDefaultTopic() {
		return this.defaultTopic;
	}

	/**
	 * 设置默认主题
	 * Set the default topic for send methods where a topic is not
	 * provided.
	 * @param defaultTopic the topic.
	 */
	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	/**
	 * 设置消息监听器，在kafka消息发送结果确认时调用它，默认提供的是LoggingProducerListener，他只记录error日志
	 * Set a {@link ProducerListener} which will be invoked when Kafka acknowledges
	 * a send operation. By default a {@link LoggingProducerListener} is configured
	 * which logs errors only.
	 * @param producerListener the listener; may be {@code null}.
	 */
	public void setProducerListener(@Nullable ProducerListener<K, V> producerListener) {
		this.producerListener = producerListener;
	}

	/**
	 * 返回消息转换器
	 * Return the message converter.
	 * @return the message converter.
	 */
	public MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * 设置消息转换器
	 * Set the message converter to use.
	 * @param messageConverter the message converter.
	 */
	public void setMessageConverter(RecordMessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.messageConverter = messageConverter;
	}

	@Override
	public boolean isTransactional() {
		return this.transactional;
	}

	/**
	 * 获取事务ID前缀
	 * @return
	 */
	public String getTransactionIdPrefix() {
		return this.transactionIdPrefix;
	}

	/**
	 * 设置事务ID前缀，用来覆盖生产者工厂中的前缀
	 * Set a transaction id prefix to override the prefix in the producer factory.
	 * @param transactionIdPrefix the prefix.
	 * @since 2.3
	 */
	public void setTransactionIdPrefix(String transactionIdPrefix) {
		this.transactionIdPrefix = transactionIdPrefix;
	}

	/**
	 * 设置关闭生产者的最长等待时间，最长为5秒
	 * Set the maximum time to wait when closing a producer; default 5 seconds.
	 * @param closeTimeout the close timeout.
	 * @since 2.1.14
	 */
	public void setCloseTimeout(Duration closeTimeout) {
		Assert.notNull(closeTimeout, "'closeTimeout' cannot be null");
		this.closeTimeout = closeTimeout;
	}

	/**
	 * 返回生产者工厂
	 * Return the producer factory used by this template.
	 * @return the factory.
	 * @since 2.2.5
	 */
	public ProducerFactory<K, V> getProducerFactory() {
		return this.producerFactory;
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(@Nullable V data) {
		//发送到默认主题
		return send(this.defaultTopic, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(K key, @Nullable V data) {
		//使用指定的密钥发送到默认主题
		return send(this.defaultTopic, key, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, K key, @Nullable V data) {
		//使用指定的密钥和分区发送到默认主题
		return send(this.defaultTopic, partition, key, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, Long timestamp, K key, @Nullable V data) {
		//使用指定的密钥和分区发送到默认主题
		return send(this.defaultTopic, partition, timestamp, key, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, @Nullable V data) {
		//创建生产者消息
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
		//发送生产者消息
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, K key, @Nullable V data) {
		//创建生产者消息
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
		//发送生产者消息
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, @Nullable V data) {
		//创建生产者消息
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
		//发送生产者消息
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, Long timestamp, K key,
			@Nullable V data) {
		//创建生产者消息
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, data);
		//发送生产者消息
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(ProducerRecord<K, V> record) {
		return doSend(record);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ListenableFuture<SendResult<K, V>> send(Message<?> message) {
		//转换消息，使用默认主题创建生产者消息
		ProducerRecord<?, ?> producerRecord = this.messageConverter.fromMessage(message, this.defaultTopic);
		//如果没有headers
		if (!producerRecord.headers().iterator().hasNext()) { // possibly no Jackson
			//从消息中获取信息头
			byte[] correlationId = message.getHeaders().get(KafkaHeaders.CORRELATION_ID, byte[].class);
			if (correlationId != null) {
				//放在生产者消息头里
				producerRecord.headers().add(KafkaHeaders.CORRELATION_ID, correlationId);
			}
		}
		//发送生产者消息
		return doSend((ProducerRecord<K, V>) producerRecord);
	}


	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		//获取生产者
		Producer<K, V> producer = getTheProducer();
		try {
			//返回生产者的位移量
			return producer.partitionsFor(topic);
		}
		finally {
			//关闭生产者
			closeProducer(producer, inTransaction());
		}
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		//获取生产者
		Producer<K, V> producer = getTheProducer();
		try {
			//获取生产者配置
			return producer.metrics();
		}
		finally {
			//关闭生产者
			closeProducer(producer, inTransaction());
		}
	}

	@Override
	public <T> T execute(ProducerCallback<K, V, T> callback) {
		Assert.notNull(callback, "'callback' cannot be null");
		//获取生产者
		Producer<K, V> producer = getTheProducer();
		try {
			//在kafka中执行操作
			return callback.doInKafka(producer);
		}
		finally {
			//关闭生产者
			closeProducer(producer, inTransaction());
		}
	}

	@Override
	public <T> T executeInTransaction(OperationsCallback<K, V, T> callback) {
		Assert.notNull(callback, "'callback' cannot be null");
		Assert.state(this.transactional, "Producer factory does not support transactions");
		//从线程变量获取生产者
		Producer<K, V> producer = this.producers.get();
		Assert.state(producer == null, "Nested calls to 'executeInTransaction' are not allowed");
		String transactionIdSuffix;
		if (this.producerFactory.isProducerPerConsumerPartition()) {
			//获取事务ID后缀
			transactionIdSuffix = TransactionSupport.getTransactionIdSuffix();
			//情况事务ID后缀
			TransactionSupport.clearTransactionIdSuffix();
		}
		else {
			transactionIdSuffix = null;
		}

		//通过事务前缀创建一个生产者
		producer = this.producerFactory.createProducer(this.transactionIdPrefix);

		try {
			//开启事务
			producer.beginTransaction();
		}
		catch (Exception e) {
			closeProducer(producer, false);
			throw e;
		}

		//将生产者放在线程变量中
		this.producers.set(producer);
		try {
			//执行操作并获取反馈
			T result = callback.doInOperations(this);
			try {
				//提交事务
				producer.commitTransaction();
			}
			catch (Exception e) {
				throw new SkipAbortException(e);
			}
			return result;
		}
		catch (SkipAbortException e) { // NOSONAR - exception flow control
			throw ((RuntimeException) e.getCause()); // NOSONAR - lost stack trace
		}
		catch (Exception e) {
			//终止事务
			producer.abortTransaction();
			throw e;
		}
		finally {
			if (transactionIdSuffix != null) {
				//事务ID后缀放在线程变量里
				TransactionSupport.setTransactionIdSuffix(transactionIdSuffix);
			}
			//移除生产者
			this.producers.remove();
			//关闭生产者
			closeProducer(producer, false);
		}
	}

	/**
	 * 只有提供单例生产者的时候调用才有意义
	 * {@inheritDoc}
	 * <p><b>Note</b> It only makes sense to invoke this method if the
	 * {@link ProducerFactory} serves up a singleton producer (such as the
	 * {@link DefaultKafkaProducerFactory}).
	 */
	@Override
	public void flush() {
		//获取生产者
		Producer<K, V> producer = getTheProducer();
		try {
			//刷新
			producer.flush();
		}
		finally {
			//关闭生产者
			closeProducer(producer, inTransaction());
		}
	}


	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets) {
		sendOffsetsToTransaction(offsets, KafkaUtils.getConsumerGroupId());
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
		//获取线程变量中的生产者
		Producer<K, V> producer = this.producers.get();
		if (producer == null) {
			//如果当前线程变量获取不到生产者，获取一个生产者
			@SuppressWarnings("unchecked")
			KafkaResourceHolder<K, V> resourceHolder = (KafkaResourceHolder<K, V>) TransactionSynchronizationManager
					.getResource(this.producerFactory);
			Assert.isTrue(resourceHolder != null, "No transaction in process");
			producer = resourceHolder.getProducer();
		}
		//向事务发送偏移量
		producer.sendOffsetsToTransaction(offsets, consumerGroupId);
	}

	protected void closeProducer(Producer<K, V> producer, boolean inTx) {
		//如果不是在事务中
		if (!inTx) {
			//关闭生产者
			producer.close(this.closeTimeout);
		}
	}

	/**
	 * 发送生产者消息
	 * Send the producer record.
	 * @param producerRecord the producer record.
	 * @return a Future for the {@link org.apache.kafka.clients.producer.RecordMetadata
	 * RecordMetadata}.
	 */
	protected ListenableFuture<SendResult<K, V>> doSend(final ProducerRecord<K, V> producerRecord) {
		//如果支持事务
		if (this.transactional) {
			//断言启动了事务
			Assert.state(inTransaction(),
					"No transaction is in process; "
						+ "possible solutions: run the template operation within the scope of a "
						+ "template.executeInTransaction() operation, start a transaction with @Transactional "
						+ "before invoking the template method, "
						+ "run in a transaction started by a listener container when consuming a record");
		}
		//获取生产者
		final Producer<K, V> producer = getTheProducer();
		this.logger.trace(() -> "Sending: " + producerRecord);
		final SettableListenableFuture<SendResult<K, V>> future = new SettableListenableFuture<>();
		//异步发送生产者消息，并设置回调函数
		producer.send(producerRecord, buildCallback(producerRecord, producer, future));
		//如果是自动刷新的就调用刷新
		if (this.autoFlush) {
			flush();
		}
		this.logger.trace(() -> "Sent: " + producerRecord);
		return future;
	}

	private Callback buildCallback(final ProducerRecord<K, V> producerRecord, final Producer<K, V> producer,
			final SettableListenableFuture<SendResult<K, V>> future) {
		return (metadata, exception) -> {
			try {
				if (exception == null) {
					//没有异常，记录发送的结果
					future.set(new SendResult<>(producerRecord, metadata));
					//如果有发送结果监听则通知发送成功
					if (KafkaTemplate.this.producerListener != null) {
						KafkaTemplate.this.producerListener.onSuccess(producerRecord, metadata);
					}
					KafkaTemplate.this.logger.trace(() -> "Sent ok: " + producerRecord + ", metadata: " + metadata);
				}
				else {
					//有异常，记录发送失败
					future.setException(new KafkaProducerException(producerRecord, "Failed to send", exception));
					if (KafkaTemplate.this.producerListener != null) {
						//如果有发送结果监听则通知发送失败
						KafkaTemplate.this.producerListener.onError(producerRecord, exception);
					}
					KafkaTemplate.this.logger.debug(exception, () -> "Failed to send: " + producerRecord);
				}
			}
			finally {
				//不再事务中，关闭生产者
				if (!KafkaTemplate.this.transactional) {
					closeProducer(producer, false);
				}
			}
		};
	}


	/**
	 * 是否以事务的形式运行
	 * Return true if the template is currently running in a transaction on the
	 * calling thread.
	 * @return true if a transaction is running.
	 * @since 2.2.1
	 */
	public boolean inTransaction() {
		//支持事务  生产者不为空  启动了事务
		return this.transactional && (this.producers.get() != null
				|| TransactionSynchronizationManager.getResource(this.producerFactory) != null
				|| TransactionSynchronizationManager.isActualTransactionActive());
	}

	private Producer<K, V> getTheProducer() {
		//如果启用事务
		if (this.transactional) {
			//获取当前线程变量中的生产者
			Producer<K, V> producer = this.producers.get();
			if (producer != null) {
				//如果当前线程有生产者直接返回
				return producer;
			}
			//没有获得生产者，通过事务ID和生产者工厂获取一个生产者
			KafkaResourceHolder<K, V> holder = ProducerFactoryUtils
					.getTransactionalResourceHolder(this.producerFactory, this.transactionIdPrefix, this.closeTimeout);
			return holder.getProducer();
		}
		else {
			//没启用事务，创建一个生产者
			return this.producerFactory.createProducer(this.transactionIdPrefix);
		}
	}

	@SuppressWarnings("serial")
	private static final class SkipAbortException extends RuntimeException {

		SkipAbortException(Throwable cause) {
			super(cause);
		}

	}

}
