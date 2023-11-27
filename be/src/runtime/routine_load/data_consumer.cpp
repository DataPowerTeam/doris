// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/routine_load/data_consumer.h"

#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <librdkafka/rdkafkacpp.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/small_file_mgr.h"
#include "service/backend_options.h"
#include "util/blocking_queue.hpp"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {

static const std::string PROP_GROUP_ID = "group.id";
// init kafka consumer will only set common configs such as
// brokers, groupid
Status KafkaDataConsumer::init(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK();
    }

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    // conf has to be deleted finally
    Defer delete_conf {[conf]() { delete conf; }};

    std::string errstr;
    auto set_conf = [&conf, &errstr](const std::string& conf_key, const std::string& conf_val) {
        RdKafka::Conf::ConfResult res = conf->set(conf_key, conf_val, errstr);
        if (res == RdKafka::Conf::CONF_UNKNOWN) {
            // ignore unknown config
            return Status::OK();
        } else if (errstr.find("not supported") != std::string::npos) {
            // some java-only properties may be passed to here, and librdkafak will return 'xxx' not supported
            // ignore it
            return Status::OK();
        } else if (res != RdKafka::Conf::CONF_OK) {
            std::stringstream ss;
            ss << "PAUSE: failed to set '" << conf_key << "', value: '" << conf_val
               << "', err: " << errstr;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        VLOG_NOTICE << "set " << conf_key << ": " << conf_val;
        return Status::OK();
    };

    RETURN_IF_ERROR(set_conf("metadata.broker.list", ctx->kafka_info->brokers));
    RETURN_IF_ERROR(set_conf("enable.partition.eof", "false"));
    RETURN_IF_ERROR(set_conf("enable.auto.offset.store", "false"));
    // TODO: set it larger than 0 after we set rd_kafka_conf_set_stats_cb()
    RETURN_IF_ERROR(set_conf("statistics.interval.ms", "0"));
    RETURN_IF_ERROR(set_conf("auto.offset.reset", "error"));
    RETURN_IF_ERROR(set_conf("socket.keepalive.enable", "true"));
    RETURN_IF_ERROR(set_conf("reconnect.backoff.ms", "100"));
    RETURN_IF_ERROR(set_conf("reconnect.backoff.max.ms", "10000"));
    RETURN_IF_ERROR(set_conf("api.version.request", config::kafka_api_version_request));
    RETURN_IF_ERROR(set_conf("api.version.fallback.ms", "0"));
    RETURN_IF_ERROR(set_conf("broker.version.fallback", config::kafka_broker_version_fallback));
    RETURN_IF_ERROR(set_conf("broker.address.ttl", "0"));
    if (config::kafka_debug != "disable") {
        RETURN_IF_ERROR(set_conf("debug", config::kafka_debug));
    }

    for (auto& item : ctx->kafka_info->properties) {
        if (starts_with(item.second, "FILE:")) {
            // file property should has format: FILE:file_id:md5
            std::vector<std::string> parts =
                    strings::Split(item.second, ":", strings::SkipWhitespace());
            if (parts.size() != 3) {
                return Status::InternalError("PAUSE: Invalid file property of kafka: " +
                                             item.second);
            }
            int64_t file_id = std::stol(parts[1]);
            std::string file_path;
            Status st = ctx->exec_env()->small_file_mgr()->get_file(file_id, parts[2], &file_path);
            if (!st.ok()) {
                return Status::InternalError("PAUSE: failed to get file for config: {}, error: {}",
                                             item.first, st.to_string());
            }
            RETURN_IF_ERROR(set_conf(item.first, file_path));
        } else {
            RETURN_IF_ERROR(set_conf(item.first, item.second));
        }
        _custom_properties.emplace(item.first, item.second);
    }

    // if not specified group id, generate a random one.
    // ATTN: In the new version, we have set a group.id on the FE side for jobs that have not set a groupid,
    // but in order to ensure compatibility, we still do a check here.
    if (_custom_properties.find(PROP_GROUP_ID) == _custom_properties.end()) {
        std::stringstream ss;
        ss << BackendOptions::get_localhost() << "_";
        std::string group_id = ss.str() + UniqueId::gen_uid().to_string();
        RETURN_IF_ERROR(set_conf(PROP_GROUP_ID, group_id));
        _custom_properties.emplace(PROP_GROUP_ID, group_id);
    }
    LOG(INFO) << "init kafka consumer with group id: " << _custom_properties[PROP_GROUP_ID];

    if (conf->set("event_cb", &_k_event_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::stringstream ss;
        ss << "PAUSE: failed to set 'event_cb'";
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // create consumer
    _k_consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!_k_consumer) {
        LOG(WARNING) << "PAUSE: failed to create kafka consumer: " << errstr;
        return Status::InternalError("PAUSE: failed to create kafka consumer: " + errstr);
    }

    VLOG_NOTICE << "finished to init kafka consumer. " << ctx->brief();

    _init = true;
    return Status::OK();
}

Status KafkaDataConsumer::assign_topic_partitions(
        const std::map<int32_t, int64_t>& begin_partition_offset, const std::string& topic,
        std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(_k_consumer);
    // create TopicPartitions
    std::stringstream ss;
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (auto& entry : begin_partition_offset) {
        RdKafka::TopicPartition* tp1 =
                RdKafka::TopicPartition::create(topic, entry.first, entry.second);
        topic_partitions.push_back(tp1);
        ss << "[" << entry.first << ": " << entry.second << "] ";
    }

    LOG(INFO) << "consumer: " << _id << ", grp: " << _grp_id
              << " assign topic partitions: " << topic << ", " << ss.str();

    // delete TopicPartition finally
    Defer delete_tp {[&topic_partitions]() {
        std::for_each(topic_partitions.begin(), topic_partitions.end(),
                      [](RdKafka::TopicPartition* tp1) { delete tp1; });
    }};

    // assign partition
    RdKafka::ErrorCode err = _k_consumer->assign(topic_partitions);
    if (err) {
        LOG(WARNING) << "failed to assign topic partitions: " << ctx->brief(true)
                     << ", err: " << RdKafka::err2str(err);
        _k_consumer->unassign();
        return Status::InternalError("failed to assign topic partitions");
    }

    return Status::OK();
}

Status KafkaDataConsumer::group_consume(BlockingQueue<RdKafka::Message*>* queue,
                                        int64_t max_running_time_ms) {
    static constexpr int MAX_RETRY_TIMES_FOR_TRANSPORT_FAILURE = 3;
    int64_t left_time = max_running_time_ms;
    LOG(INFO) << "start kafka consumer: " << _id << ", grp: " << _grp_id
              << ", max running time(ms): " << left_time;

    int64_t received_rows = 0;
    int64_t put_rows = 0;
    int32_t retry_times = 0;
    Status st = Status::OK();
    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();
    while (true) {
        {
            std::unique_lock<std::mutex> l(_lock);
            if (_cancelled) {
                break;
            }
        }

        if (left_time <= 0) {
            break;
        }

        bool done = false;
        // consume 1 message at a time
        consumer_watch.start();
        std::unique_ptr<RdKafka::Message> msg(_k_consumer->consume(1000 /* timeout, ms */));
        consumer_watch.stop();
        switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            if (msg->len() == 0) {
                // ignore msg with length 0.
                // put empty msg into queue will cause the load process shutting down.
                break;
            } else if (!queue->blocking_put(msg.get())) {
                // queue is shutdown
                done = true;
            } else {
                ++put_rows;
                msg.release(); // release the ownership, msg will be deleted after being processed
            }
            ++received_rows;
            break;
        case RdKafka::ERR__TIMED_OUT:
            // leave the status as OK, because this may happened
            // if there is no data in kafka.
            LOG(INFO) << "kafka consume timeout: " << _id;
            break;
        case RdKafka::ERR__TRANSPORT:
            LOG(INFO) << "kafka consume Disconnected: " << _id
                      << ", retry times: " << retry_times++;
            if (retry_times <= MAX_RETRY_TIMES_FOR_TRANSPORT_FAILURE) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                break;
            }
            [[fallthrough]];
        default:
            LOG(WARNING) << "kafka consume failed: " << _id << ", msg: " << msg->errstr();
            done = true;
            st = Status::InternalError(msg->errstr());
            break;
        }

        left_time = max_running_time_ms - watch.elapsed_time() / 1000 / 1000;
        if (done) {
            break;
        }
    }

    LOG(INFO) << "kafka consumer done: " << _id << ", grp: " << _grp_id
              << ". cancelled: " << _cancelled << ", left time(ms): " << left_time
              << ", total cost(ms): " << watch.elapsed_time() / 1000 / 1000
              << ", consume cost(ms): " << consumer_watch.elapsed_time() / 1000 / 1000
              << ", received rows: " << received_rows << ", put rows: " << put_rows;

    return st;
}

Status KafkaDataConsumer::get_partition_meta(std::vector<int32_t>* partition_ids) {
    // create topic conf
    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    Defer delete_conf {[tconf]() { delete tconf; }};

    // create topic
    std::string errstr;
    RdKafka::Topic* topic = RdKafka::Topic::create(_k_consumer, _topic, tconf, errstr);
    if (topic == nullptr) {
        std::stringstream ss;
        ss << "failed to create topic: " << errstr;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    Defer delete_topic {[topic]() { delete topic; }};

    // get topic metadata
    RdKafka::Metadata* metadata = nullptr;
    RdKafka::ErrorCode err =
            _k_consumer->metadata(true /* for this topic */, topic, &metadata, 5000);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::stringstream ss;
        ss << "failed to get partition meta: " << RdKafka::err2str(err);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    Defer delete_meta {[metadata]() { delete metadata; }};

    // get partition ids
    RdKafka::Metadata::TopicMetadataIterator it;
    for (it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it) {
        if ((*it)->topic() != _topic) {
            continue;
        }

        if ((*it)->err() != RdKafka::ERR_NO_ERROR) {
            std::stringstream ss;
            ss << "error: " << err2str((*it)->err());
            if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE) {
                ss << ", try again";
            }
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        RdKafka::TopicMetadata::PartitionMetadataIterator ip;
        for (ip = (*it)->partitions()->begin(); ip != (*it)->partitions()->end(); ++ip) {
            partition_ids->push_back((*ip)->id());
        }
    }

    if (partition_ids->empty()) {
        return Status::InternalError("no partition in this topic");
    }

    return Status::OK();
}

// get offsets of each partition for times.
// The input parameter "times" holds <partition, timestamps>
// The output parameter "offsets" returns <partition, offsets>
//
// The returned offset for each partition is the earliest offset whose
// timestamp is greater than or equal to the given timestamp in the
// corresponding partition.
// See librdkafka/rdkafkacpp.h##offsetsForTimes()
Status KafkaDataConsumer::get_offsets_for_times(const std::vector<PIntegerPair>& times,
                                                std::vector<PIntegerPair>* offsets) {
    // create topic partition
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (const auto& entry : times) {
        RdKafka::TopicPartition* tp1 =
                RdKafka::TopicPartition::create(_topic, entry.key(), entry.val());
        topic_partitions.push_back(tp1);
    }
    // delete TopicPartition finally
    Defer delete_tp {[&topic_partitions]() {
        std::for_each(topic_partitions.begin(), topic_partitions.end(),
                      [](RdKafka::TopicPartition* tp1) { delete tp1; });
    }};

    // get offsets for times
    RdKafka::ErrorCode err = _k_consumer->offsetsForTimes(topic_partitions, 5000);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::stringstream ss;
        ss << "failed to get offsets for times: " << RdKafka::err2str(err);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    for (const auto& topic_partition : topic_partitions) {
        PIntegerPair pair;
        pair.set_key(topic_partition->partition());
        pair.set_val(topic_partition->offset());
        offsets->push_back(std::move(pair));
    }

    return Status::OK();
}

// get latest offsets for given partitions
Status KafkaDataConsumer::get_latest_offsets_for_partitions(
        const std::vector<int32_t>& partition_ids, std::vector<PIntegerPair>* offsets) {
    for (int32_t partition_id : partition_ids) {
        int64_t low = 0;
        int64_t high = 0;
        RdKafka::ErrorCode err =
                _k_consumer->query_watermark_offsets(_topic, partition_id, &low, &high, 5000);
        if (err != RdKafka::ERR_NO_ERROR) {
            std::stringstream ss;
            ss << "failed to get latest offset for partition: " << partition_id
               << ", err: " << RdKafka::err2str(err);
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        PIntegerPair pair;
        pair.set_key(partition_id);
        pair.set_val(high);
        offsets->push_back(std::move(pair));
    }

    return Status::OK();
}

Status KafkaDataConsumer::cancel(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status::InternalError("consumer is not initialized");
    }

    _cancelled = true;
    LOG(INFO) << "kafka consumer cancelled. " << _id;
    return Status::OK();
}

Status KafkaDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
    _cancelled = false;
    _k_consumer->unassign();
    // reset will be called before this consumer being returned to the pool.
    // so update _last_visit_time is reasonable.
    _last_visit_time = time(nullptr);
    return Status::OK();
}

Status KafkaDataConsumer::commit(std::vector<RdKafka::TopicPartition*>& offset) {
    // Use async commit so that it will not block for a long time.
    // Commit failure has no effect on Doris, subsequent tasks will continue to commit the new offset
    RdKafka::ErrorCode err = _k_consumer->commitAsync(offset);
    if (err != RdKafka::ERR_NO_ERROR) {
        return Status::InternalError("failed to commit kafka offset : {}", RdKafka::err2str(err));
    }
    return Status::OK();
}

// if the kafka brokers and topic are same,
// we considered this consumer as matched, thus can be reused.
bool KafkaDataConsumer::match(std::shared_ptr<StreamLoadContext> ctx) {
    if (ctx->load_src_type != TLoadSourceType::KAFKA) {
        return false;
    }
    if (_brokers != ctx->kafka_info->brokers || _topic != ctx->kafka_info->topic) {
        return false;
    }
    // check properties
    if (_custom_properties.size() != ctx->kafka_info->properties.size()) {
        return false;
    }
    for (auto& item : ctx->kafka_info->properties) {
        std::unordered_map<std::string, std::string>::const_iterator itr =
                _custom_properties.find(item.first);
        if (itr == _custom_properties.end()) {
            return false;
        }

        if (itr->second != item.second) {
            return false;
        }
    }
    return true;
}

// init pulsar consumer will only set common configs
Status PulsarDataConsumer::init(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK();
    }

    pulsar::ClientConfiguration config;
    for (auto& item : ctx->pulsar_info->properties) {
        if (item.first == "auth.token") {
            config.setAuth(pulsar::AuthToken::createWithToken(item.second));
        } else {
            LOG(WARNING) << "Config " << item.first << " not supported for now.";
        }

        _custom_properties.emplace(item.first, item.second);
    }

    _p_client = new pulsar::Client(_service_url, config);

    VLOG(3) << "finished to init pulsar consumer. " << ctx->brief();

    _init = true;
    return Status::OK();
}

Status PulsarDataConsumer::assign_partition(const std::string& partition, std::shared_ptr<StreamLoadContext> ctx,
                                            int64_t initial_position) {
    DCHECK(_p_client);

    std::stringstream ss;
    ss << "consumer: " << _id << ", grp: " << _grp_id << " topic: " << _topic
       << ", subscription: " << _subscription << ", initial_position: " << initial_position;
    LOG(INFO) << ss.str();

    // do subscribe
    pulsar::ConsumerConfiguration conf;
    conf.setConsumerType(pulsar::ConsumerExclusive);

    pulsar::Result result;
    result = _p_client->subscribe(partition, _subscription, conf, _p_consumer);
    if (result != pulsar::ResultOk) {
        LOG(WARNING) << "PAUSE: failed to create pulsar consumer: " << ctx->brief(true) << ", err: " << result;
        return Status::InternalError("PAUSE: failed to create pulsar consumer: " +
                                     std::string(pulsar::strResult(result)));
    }

    if (initial_position == InitialPosition::LATEST || initial_position == InitialPosition::EARLIEST) {
        pulsar::InitialPosition p_initial_position = initial_position == InitialPosition::LATEST
                                                             ? pulsar::InitialPosition::InitialPositionLatest
                                                             : pulsar::InitialPosition::InitialPositionEarliest;
        result = _p_consumer.seek(p_initial_position);
        if (result != pulsar::ResultOk) {
            LOG(WARNING) << "PAUSE: failed to reset the subscription: " << ctx->brief(true) << ", err: " << result;
            return Status::InternalError("PAUSE: failed to reset the subscription: " +
                                         std::string(pulsar::strResult(result)));
        }
    }

    return Status::OK();
}

Status PulsarDataConsumer::group_consume(BlockingQueue<pulsar::Message*>* queue, int64_t max_running_time_ms) {
    _last_visit_time = time(nullptr);
    int64_t left_time = max_running_time_ms;
    LOG(INFO) << "start pulsar consumer: " << _id << ", grp: " << _grp_id << ", max running time(ms): " << left_time;

    int64_t received_rows = 0;
    int64_t put_rows = 0;
    Status st = Status::OK();
    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();
    while (true) {
        {
            std::unique_lock<std::mutex> l(_lock);
            if (_cancelled) {
                break;
            }
        }

        if (left_time <= 0) {
            break;
        }

        bool done = false;
        auto msg = std::make_unique<pulsar::Message>();
        std::vector<char*> rows;
        char* filter_data;
        // consume 1 message at a time
        consumer_watch.start();
        pulsar::Result res = _p_consumer.receive(*(msg.get()), 30000 /* timeout, ms */);
        consumer_watch.stop();
        switch (res) {
        case pulsar::ResultOk:
            if (msg.get()->getDataAsString().find("\"country\":\"PL\"") != std::string::npos) {
                LOG(INFO) << "receive pulsar message: " << msg.get()->getDataAsString()
                          << ", message id: " << msg.get()->getMessageId()
                          << ", len: " << msg.get()->getLength();
                //filter invalid prefix of json
                filter_data =
                    filter_invalid_prefix_of_json(static_cast<char*>(msg->getData()), msg.get()->getLength());
                rows = convert_rows(filter_data);
                for (char* row : rows) {
                    pulsar::MessageBuilder messageBuilder;
                    size_t row_len = len_of_actual_data(row);
                    messageBuilder.setContent(row, row_len);
                    messageBuilder.setProperty("topicName",msg.get()->getTopicName());
                    pulsar::Message new_msg = messageBuilder.build();
                    std::string partition = new_msg.getProperty("topicName");
                    new_msg.setMessageId(msg.get()->getMessageId());
                    pulsar::MessageId msg_id = new_msg.getMessageId();
                    std::size_t msg_len = new_msg.getLength();
                    LOG(INFO) << "get pulsar message: " << std::string(row, row_len)
                              << ", partition: " << partition << ", message id: " << msg_id
                              << ", len: " << msg_len << ", filter_len: " << row_len
                              << ", size: " << rows.size()
                              << ", bool topicName: " << new_msg.hasProperty("topicName")
                              << ", value topicName: " << new_msg.getProperty("topicName");
                }
                for (char* row : rows) {
                    delete[] row;
                }
                rows.clear();
                delete[] filter_data;
            }
            if (msg.get()->getDataAsString().find("{\"") == std::string::npos) {
                // ignore msg with length 0.
                // put empty msg into queue will cause the load process shutting down.
                LOG(INFO) << "pass null message: " << msg.get()->getDataAsString();
                break;
            } else if (!queue->blocking_put(msg.get())) {
                // queue is shutdown
                done = true;
            } else {
                ++put_rows;
                msg.release(); // release the ownership, msg will be deleted after being processed
            }
            ++received_rows;
            break;
        case pulsar::ResultTimeout:
            // leave the status as OK, because this may happened
            // if there is no data in pulsar.
            LOG(INFO) << "pulsar consumer"
                      << "[" << _id << "]"
                      << " consume timeout.";
            break;
        default:
            LOG(WARNING) << "pulsar consumer"
                         << "[" << _id << "]"
                         << " consume failed: "
                         << ", errmsg: " << res;
            done = true;
            st = Status::InternalError(pulsar::strResult(res));
            break;
        }

        left_time = max_running_time_ms - watch.elapsed_time() / 1000 / 1000;
        if (done) {
            break;
        }
    }

    LOG(INFO) << "pulsar consume done: " << _id << ", grp: " << _grp_id << ". cancelled: " << _cancelled
              << ", left time(ms): " << left_time << ", total cost(ms): " << watch.elapsed_time() / 1000 / 1000
              << ", consume cost(ms): " << consumer_watch.elapsed_time() / 1000 / 1000
              << ", received rows: " << received_rows << ", put rows: " << put_rows;

    return st;
}

const std::string& PulsarDataConsumer::get_partition() {
    _last_visit_time = time(nullptr);
    return _p_consumer.getTopic();
}

Status PulsarDataConsumer::get_partition_backlog(int64_t* backlog) {
    _last_visit_time = time(nullptr);
    pulsar::BrokerConsumerStats broker_consumer_stats;
    pulsar::Result result = _p_consumer.getBrokerConsumerStats(broker_consumer_stats);
    if (result != pulsar::ResultOk) {
        LOG(WARNING) << "Failed to get broker consumer stats: "
                     << ", err: " << result;
        return Status::InternalError("Failed to get broker consumer stats: " + std::string(pulsar::strResult(result)));
    }
    *backlog = broker_consumer_stats.getMsgBacklog();

    return Status::OK();
}

Status PulsarDataConsumer::get_topic_partition(std::vector<std::string>* partitions) {
    _last_visit_time = time(nullptr);
    pulsar::Result result = _p_client->getPartitionsForTopic(_topic, *partitions);
    if (result != pulsar::ResultOk) {
        LOG(WARNING) << "Failed to get partitions for topic: " << _topic << ", err: " << result;
        return Status::InternalError("Failed to get partitions for topic: " + std::string(pulsar::strResult(result)));
    }

    return Status::OK();
}

Status PulsarDataConsumer::cancel(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status::InternalError("consumer is not initialized");
    }

    _cancelled = true;
    LOG(INFO) << "pulsar consumer cancelled. " << _id;
    return Status::OK();
}

Status PulsarDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
    _cancelled = false;
    _p_consumer.close();
    return Status::OK();
}

Status PulsarDataConsumer::acknowledge_cumulative(pulsar::MessageId& message_id) {
    pulsar::Result res = _p_consumer.acknowledgeCumulative(message_id);
    if (res != pulsar::ResultOk) {
        std::stringstream ss;
        ss << "failed to acknowledge pulsar message : " << res;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

bool PulsarDataConsumer::match(std::shared_ptr<StreamLoadContext> ctx) {
    if (ctx->load_src_type != TLoadSourceType::PULSAR) {
        return false;
    }
    if (_service_url != ctx->pulsar_info->service_url || _topic != ctx->pulsar_info->topic ||
        _subscription != ctx->pulsar_info->subscription) {
        return false;
    }
    // check properties
    if (_custom_properties.size() != ctx->pulsar_info->properties.size()) {
        return false;
    }
    for (auto& item : ctx->pulsar_info->properties) {
        auto it = _custom_properties.find(item.first);
        if (it == _custom_properties.end() || it->second != item.second) {
            return false;
        }
    }

    return true;
}

char* PulsarDataConsumer::filter_invalid_prefix_of_json(char* data, std::size_t size) {
    // first index of '{'
    int first_left_curly_bracket_index  = -1;
    for (int i = 0; i < size; ++i) {
        if (first_left_curly_bracket_index == -1 && data[i] == '{') {
            first_left_curly_bracket_index = i;
            if ( i+1 < size && data[i+1] == '{') {
                first_left_curly_bracket_index = i + 1;
            }
            break;
        }
    }
    if (first_left_curly_bracket_index >= 0) {
        return data + first_left_curly_bracket_index;
    } else {
        return data;
    }
}

size_t PulsarDataConsumer::len_of_actual_data(char* data) {
    size_t length = 0;
    while (data[length] != '\0') {
        ++length;
    }
    return length;
}

std::vector<char*> PulsarDataConsumer::convert_rows(char* data) {
    std::vector<char*> targets;
    rapidjson::Document source;
    rapidjson::Document destination;
    rapidjson::StringBuffer buffer;
    if(!source.Parse(data).HasParseError()) {
        if (source.HasMember("events") && source["events"].IsArray()) {
            const rapidjson::Value& array = source["events"];
            size_t len = array.Size();
            for(size_t i = 0; i < len; i++) {
                destination.SetObject();
                rapidjson::Value& object = const_cast<rapidjson::Value&>(array[i]);
                rapidjson::Value eventName("event", destination.GetAllocator());
                destination.AddMember(eventName, object, destination.GetAllocator());
                for (auto& member : source.GetObject()) {
                    const char* key = member.name.GetString();
                    if (std::strcmp(key, "events") != 0) {
                        rapidjson::Value keyName(key, destination.GetAllocator());
                        rapidjson::Value& sourceValue = source[key];
                        destination.AddMember(keyName, sourceValue, destination.GetAllocator());
                    }
                }

                buffer.Clear();
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                destination.Accept(writer);

                size_t buffer_size = buffer.GetSize();
                char* dest_string = new char[buffer_size + 1];
                std::memcpy(dest_string, buffer.GetString(), buffer_size);
                dest_string[buffer_size] = '\0';
                targets.push_back(dest_string);

                destination.RemoveAllMembers();
            }
        } else {
            targets.push_back(data);
        }
    } else {
        targets.push_back(data);
    }
    destination.Clear();
    rapidjson::Document().Swap(destination);
    source.Clear();
    rapidjson::Document().Swap(source);
    return targets;
}

} // end namespace doris
