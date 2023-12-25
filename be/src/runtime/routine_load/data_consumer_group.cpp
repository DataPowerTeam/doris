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
#include "runtime/routine_load/data_consumer_group.h"

#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>

#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <chrono> // IWYU pragma: keep
#include <sstream>
#include <algorithm>

#include "common/logging.h"
#include "librdkafka/rdkafkacpp.h"
#include "runtime/routine_load/data_consumer.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/stopwatch.hpp"

namespace doris {

Status KafkaDataConsumerGroup::assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(ctx->kafka_info);
    DCHECK(_consumers.size() >= 1);

    // divide partitions
    int consumer_size = _consumers.size();
    std::vector<std::map<int32_t, int64_t>> divide_parts(consumer_size);
    int i = 0;
    for (auto& kv : ctx->kafka_info->begin_offset) {
        int idx = i % consumer_size;
        divide_parts[idx].emplace(kv.first, kv.second);
        i++;
    }

    // assign partitions to consumers equally
    for (int i = 0; i < consumer_size; ++i) {
        RETURN_IF_ERROR(
                std::static_pointer_cast<KafkaDataConsumer>(_consumers[i])
                        ->assign_topic_partitions(divide_parts[i], ctx->kafka_info->topic, ctx));
    }

    return Status::OK();
}

KafkaDataConsumerGroup::~KafkaDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        RdKafka::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status KafkaDataConsumerGroup::start_all(std::shared_ptr<StreamLoadContext> ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer(std::bind<void>(
                    &KafkaDataConsumerGroup::actual_consume, this, consumer, &_queue,
                    ctx->max_interval_s * 1000, [this, &result_st](const Status& st) {
                        std::unique_lock<std::mutex> lock(_mutex);
                        _counter--;
                        VLOG_CRITICAL << "group counter is: " << _counter << ", grp: " << _grp_id;
                        if (_counter == 0) {
                            _queue.shutdown();
                            LOG(INFO) << "all consumers are finished. shutdown queue. group id: "
                                      << _grp_id;
                        }
                        if (result_st.ok() && !st.ok()) {
                            result_st = st;
                        }
                    }))) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id()
                         << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG_CRITICAL << "submit a data consumer: " << consumer->id()
                          << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t left_rows = ctx->max_batch_rows;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<io::KafkaConsumerPipe> kafka_pipe =
            std::static_pointer_cast<io::KafkaConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch rows: " << left_rows << ", batch size: " << left_bytes << ". "
              << ctx->brief();

    // copy one
    std::map<int32_t, int64_t> cmt_offset = ctx->kafka_info->cmt_offset;

    //improve performance
    Status (io::KafkaConsumerPipe::*append_data)(const char* data, size_t size);
    if (ctx->format == TFileFormatType::FORMAT_JSON) {
        append_data = &io::KafkaConsumerPipe::append_json;
    } else {
        append_data = &io::KafkaConsumerPipe::append_with_line_delimiter;
    }

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_rows <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << ctx->max_batch_rows - left_rows
                      << ", received bytes=" << ctx->max_batch_size - left_bytes << ", eos: " << eos
                      << ", left_time: " << left_time << ", left_rows: " << left_rows
                      << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000 << ", "
                      << ctx->brief();

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                consumer->cancel(ctx);
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();
            if (!result_st.ok()) {
                kafka_pipe->cancel(result_st.to_string());
                return result_st;
            }
            kafka_pipe->finish();
            ctx->kafka_info->cmt_offset = std::move(cmt_offset);
            ctx->receive_bytes = ctx->max_batch_size - left_bytes;
            return Status::OK();
        }

        RdKafka::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            VLOG_NOTICE << "get kafka message"
                        << ", partition: " << msg->partition() << ", offset: " << msg->offset()
                        << ", len: " << msg->len();

            Status st = (kafka_pipe.get()->*append_data)(static_cast<const char*>(msg->payload()),
                                                         static_cast<size_t>(msg->len()));
            if (st.ok()) {
                left_rows--;
                left_bytes -= msg->len();
                cmt_offset[msg->partition()] = msg->offset();
                VLOG_NOTICE << "consume partition[" << msg->partition() << " - " << msg->offset()
                            << "]";
            } else {
                // failed to append this msg, we must stop
                LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id;
                eos = true;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if (result_st.ok()) {
                        result_st = st;
                    }
                }
            }
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void KafkaDataConsumerGroup::actual_consume(std::shared_ptr<DataConsumer> consumer,
                                            BlockingQueue<RdKafka::Message*>* queue,
                                            int64_t max_running_time_ms, ConsumeFinishCallback cb) {
    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->group_consume(
            queue, max_running_time_ms);
    cb(st);
}

Status PulsarDataConsumerGroup::assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(ctx->pulsar_info);
    DCHECK(_consumers.size() >= 1);
    // Cumulative acknowledgement when consuming partitioned topics is not supported by pulsar
    DCHECK(_consumers.size() == ctx->pulsar_info->partitions.size());

    // assign partition to consumers
    int consumer_size = _consumers.size();
    for (int i = 0; i < consumer_size; ++i) {
        auto iter = ctx->pulsar_info->initial_positions.find(ctx->pulsar_info->partitions[i]);
        if (iter != ctx->pulsar_info->initial_positions.end()) {
            RETURN_IF_ERROR(std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])
                                    ->assign_partition(ctx->pulsar_info->partitions[i], ctx, iter->second));
        } else {
            RETURN_IF_ERROR(std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])
                                    ->assign_partition(ctx->pulsar_info->partitions[i], ctx));
        }
    }

    return Status::OK();
}

PulsarDataConsumerGroup::~PulsarDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        pulsar::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status PulsarDataConsumerGroup::start_all(std::shared_ptr<StreamLoadContext> ctx) {
    Status result_st = Status::OK();

    LOG(INFO) << "group _consumers size is: " << _consumers.size();

    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer([this, consumer, capture0 = &_queue, capture1 = ctx->max_interval_s * 1000,
                capture2 = [this, &result_st](const Status& st) {
                 std::unique_lock<std::mutex> lock(_mutex);
                 _counter--;
                 VLOG(1) << "group counter is: " << _counter << ", grp: " << _grp_id;
                 if (_counter == 0) {
                     _queue.shutdown();
                     LOG(INFO)
                             << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                 }
                 if (result_st.ok() && !st.ok()) {
                     result_st = st;
                 }
                }] { actual_consume(consumer, capture0, capture1, capture2); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(1) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<io::PulsarConsumerPipe> pulsar_pipe = std::static_pointer_cast<io::PulsarConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    std::map<std::string, pulsar::MessageId> ack_offset;

    // copy one
    std::map<std::string, pulsar::MessageId> last_ack_offset = ctx->pulsar_info->ack_offset;
    std::set<std::string> exist_repeated_ack;
    //init _filter_event_ids
    for (auto& item : ctx->pulsar_info->properties) {
        if (item.first == "event.ids") {
            init_filter_event_ids(item.second);
            LOG(INFO) << "init vector _filter_event_ids. size: " << init_filter_event_ids.size()
                      << "string: " << item.second;
        }
    }

    //improve performance
    Status (io::PulsarConsumerPipe::*append_data)(const char* data, size_t size);
    if (ctx->format == TFileFormatType::FORMAT_JSON) {
        append_data = &io::PulsarConsumerPipe::append_json;
    } else {
        append_data = &io::PulsarConsumerPipe::append_with_line_delimiter;
    }

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << received_rows << ", received bytes=" << ctx->max_batch_size - left_bytes
                      << ", eos: " << eos << ", left_time: " << left_time << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                static_cast<void>(consumer->cancel(ctx));
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some consumers encounter errors, cancel this task
                LOG(WARNING) << "Failed. Cancelled append msg to pipe. grp: " << _grp_id;
                pulsar_pipe->cancel(result_st.to_string());
                return result_st;
            }

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data
                LOG(WARNING) << "Cancelled append msg to pulsar pipe. grp: " << _grp_id;
                pulsar_pipe->cancel("Cancelled");
                return Status::InternalError("Cancelled");
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                static_cast<void>(pulsar_pipe->finish());
                ctx->pulsar_info->ack_offset = std::move(ack_offset);
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                get_backlog_nums(ctx);
//                acknowledge_cumulative(ctx);
                return Status::OK();
            }
        }

        pulsar::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            std::string partition = msg->getTopicName();
            pulsar::MessageId msg_id = msg->getMessageId();
            std::size_t len = msg->getLength();

            // avoid repeated ack
            if (exist_repeated_ack.find(partition) == exist_repeated_ack.end()) {
                if (last_ack_offset.find(partition) != last_ack_offset.end() && last_ack_offset[partition] >= msg_id) {
                    LOG(INFO) << "Pass repeated message id: " << msg_id;
//                    acknowledge(msg_id, partition);
                    left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
                    continue;
                } else {
                    exist_repeated_ack.insert(partition);
                }
            }

            //filter invalid prefix of json
            std::string filter_data = substring_prefix_json(msg->getDataAsString());
            std::vector<const char*>  rows = convert_rows(filter_data.c_str());

            VLOG(3)   << "get pulsar message:" << msg->getDataAsString()
                      << ", partition: " << partition << ", message id: " << msg_id
                      << ", len: " << len << ", size: " << msg->getDataAsString().size();

            Status st;
            for(const char* row : rows) {
                bool is_filter = is_filter_event_ids(row);
                if (!is_filter) {
                    continue;
                }
                size_t row_len = len_of_actual_data(row);
                st = (pulsar_pipe.get()->*append_data)(row, row_len);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id << ", row =" << row;
                    break;
                }
            }

           if (st.ok()) {
               received_rows++;
               // len of receive origin message from pulsar
               left_bytes -= len;
               if (ack_offset[partition] >= msg_id) {
                  LOG(WARNING) << "find repeated message id: " << msg_id;
               }
               ack_offset[partition] = msg_id;
//               acknowledge(msg_id, partition);
               VLOG(3) << "load message id: " << msg_id;
           } else {
               // failed to append this msg, we must stop
               LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id << ", errmsg=" << st.to_string();
               eos = true;
               {
                   std::unique_lock<std::mutex> lock(_mutex);
                   if (result_st.ok()) {
                       result_st = st;
                   }
               }
           }
           for(const char* row : rows) {
                delete[] row;
           }
           rows.clear();
           delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    exist_repeated_ack.clear();

    return Status::OK();
}

void PulsarDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer,
                                             BlockingQueue<pulsar::Message*>* queue, int64_t max_running_time_ms,
                                             const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->group_consume(queue, max_running_time_ms);
    cb(st);
}

void PulsarDataConsumerGroup::get_backlog_nums(std::shared_ptr<StreamLoadContext> ctx) {
    for (auto& consumer : _consumers) {
        // get backlog num
        int64_t backlog_num;
        Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition_backlog(&backlog_num);
        if (!st.ok()) {
            LOG(WARNING) << st.to_string();
        } else {
            ctx->pulsar_info
                    ->partition_backlog[std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition()] =
                    backlog_num;
        }
    }
}

Status PulsarDataConsumerGroup::acknowledge_cumulative(std::shared_ptr<StreamLoadContext> ctx) {
    Status result_st = Status::OK();
    for (auto& kv : ctx->pulsar_info->ack_offset) {
        for (auto& consumer : _consumers) {
            // do cumulative ack
            Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->acknowledge_cumulative(kv.second, kv.first);
            if (!st.ok()) {
                result_st = st;
            }
        }
    }
    return result_st;
}

void PulsarDataConsumerGroup::acknowledge(pulsar::MessageId& message_id, std::string partition) {
    for (auto& consumer : _consumers) {
        // do ack
        Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->acknowledge(message_id, partition);
        if (!st.ok()) {
            LOG(WARNING) << "failed to ack message id: " << message_id << ", consumer: " << consumer;
        }
    }
}

std::string PulsarDataConsumerGroup::substring_prefix_json(std::string data) {
    // 找到以 "{" 开头的位置
    size_t startPos = data.find("{\"");

    // 如果找到了，则截取并返回子字符串
    if (startPos != std::string::npos) {
        return data.substr(startPos);
    } else {
        return data;
    }
}

size_t PulsarDataConsumerGroup::len_of_actual_data(const char* data) {
    size_t length = 0;
    while (data[length] != '\0') {
        ++length;
    }
    return length;
}

std::vector<const char*> PulsarDataConsumerGroup::convert_rows(const char* data) {
    std::vector<const char*> targets;
    rapidjson::Document source;
    rapidjson::Document destination;
    rapidjson::StringBuffer buffer;
    if(!source.Parse(data).HasParseError()) {
        if (source.HasMember("events") && source["events"].IsArray()) {
            const rapidjson::Value& array = source["events"];
            size_t len = array.Size();
            for(size_t i = 0; i < len; i++) {
                destination.SetObject();
                for (auto& member : source.GetObject()) {
                    const char* key = member.name.GetString();
                    if (std::strcmp(key, "events") != 0) {
                        rapidjson::Value keyName(key, destination.GetAllocator());
                        rapidjson::Value sourceValue(rapidjson::kObjectType);
                        sourceValue.CopyFrom(source[key], destination.GetAllocator());
                        destination.AddMember(keyName, sourceValue, destination.GetAllocator());
                    } else {
                        rapidjson::Value& object = const_cast<rapidjson::Value&>(array[i]);
                        rapidjson::Value eventName("event", destination.GetAllocator());
                        destination.AddMember(eventName, object, destination.GetAllocator());
                    }
                }

                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                destination.Accept(writer);

                size_t buffer_size = buffer.GetSize();
                char* dest_string = new char[buffer_size + 1];
                std::memcpy(dest_string, buffer.GetString(), buffer_size);
                dest_string[buffer_size] = '\0';
                targets.push_back(dest_string);

                buffer.Clear();
                destination.Clear();
            }
        } else {
            targets.push_back(data);
        }
    } else {
        LOG(WARNING) << "Failed to convert rows, pass json: " << data;
    }
    destination.Clear();
    rapidjson::Document().Swap(destination);
    source.Clear();
    rapidjson::Document().Swap(source);
    return targets;
}

bool PulsarDataConsumerGroup::is_filter_event_ids(const char* data) {
    if (_filter_event_ids.empty()) {
        return true;
    }
    for (const char* event_id : _filter_event_ids) {
        if (strstr(data, event_id) != nullptr) {
            return true;
        }
    }
    return false;
}

void PulsarDataConsumerGroup::init_filter_event_ids(const std::string& input) {
    std::vector<std::string> splitResult;
    std::istringstream ss(input);
    std::string token;
    while (std::getline(ss, token, ',')) {
        splitResult.push_back(token);
    }
    _filter_event_ids.reserve(splitResult.size());

    std::transform(splitResult.begin(), splitResult.end(), std::back_inserter(_filter_event_ids),
            [](const std::string& str) { return str.c_str(); });
}

} // namespace doris
