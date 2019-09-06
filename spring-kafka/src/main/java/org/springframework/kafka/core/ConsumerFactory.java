/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.lang.Nullable;

/**
 * 消费者工厂类
 * The strategy to produce a {@link Consumer} instance(s).
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 */
public interface ConsumerFactory<K, V> {

	/**
	 * 创建一个消费者，使用配置中的组id和客户端id
	 * Create a consumer with the group id and client id as configured in the properties.
	 * @return the consumer.
	 */
	default Consumer<K, V> createConsumer() {
		return createConsumer(null);
	}

	/**
	 * 创建一个消费者，使用配置中的组id，在配置的客户端id后追加后缀
	 * Create a consumer, appending the suffix to the {@code client.id} property,
	 * if present.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 1.3
	 */
	default Consumer<K, V> createConsumer(@Nullable String clientIdSuffix) {
		return createConsumer(null, clientIdSuffix);
	}

	/**
	 * 创建一个消费者，使用指定的组id，在配置的客户端id后追加后缀
	 * Create a consumer with an explicit group id; in addition, the
	 * client id suffix is appended to the {@code client.id} property, if both
	 * are present.
	 * @param groupId the group id.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 1.3
	 */
	default Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdSuffix) {
		return createConsumer(groupId, null, clientIdSuffix);
	}

	/**
	 * 创建一个消费者，使用指定的组id，在配置的客户端id后追加前缀和后缀
	 * Create a consumer with an explicit group id; in addition, the
	 * client id suffix is appended to the clientIdPrefix which overrides the
	 * {@code client.id} property, if present.
	 * @param groupId the group id.
	 * @param clientIdPrefix the prefix.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 2.1.1
	 */
	Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix);

	/**
	 * 创建一个消费者，使用指定的组id，使用指定的配置并对客户端id追加前缀
	 * Create a consumer with an explicit group id; in addition, the
	 * client id suffix is appended to the clientIdPrefix which overrides the
	 * {@code client.id} property, if present. In addition, consumer properties can
	 * be overridden if the factory implementation supports it.
	 * @param groupId the group id.
	 * @param clientIdPrefix the prefix.
	 * @param clientIdSuffix the suffix.
	 * @param properties the properties to override.
	 * @return the consumer.
	 * @since 2.2.4
	 */
	default Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix, @Nullable Properties properties) {

		return createConsumer(groupId, clientIdPrefix, clientIdSuffix);
	}

	/**
	 * 返回此工厂创建的消费者是不是自动提交的
	 * Return true if consumers created by this factory use auto commit.
	 * @return true if auto commit.
	 */
	boolean isAutoCommit();

	/**
	 * 返回工厂的配置，此配置是无法修改的
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * 对克隆工厂很有用
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 2.0
	 */
	default Map<String, Object> getConfigurationProperties() {
		throw new UnsupportedOperationException("'getConfigurationProperties()' is not supported");
	}

	/**
	 * 返回配置key的反序列化程序（如果在属性值是作为对象而不是类名提供的）
	 * Return the configured key deserializer (if provided as an object instead
	 * of a class name in the properties).
	 * @return the deserializer.
	 * @since 2.0
	 */
	@Nullable
	default Deserializer<K> getKeyDeserializer() {
		return null;
	}

	/**
	 * 返回配置value的反序列化程序（如果在属性值是作为对象而不是类名提供的）
	 * Return the configured value deserializer (if provided as an object instead
	 * of a class name in the properties).
	 * @return the deserializer.
	 * @since 2.0
	 */
	@Nullable
	default Deserializer<V> getValueDeserializer() {
		return null;
	}

}
