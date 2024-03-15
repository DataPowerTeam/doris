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

#include <chrono> // IWYU pragma: keep
#include <map>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <iomanip>

#include "common/logging.h"
#include "librdkafka/rdkafkacpp.h"
#include "runtime/routine_load/data_consumer.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/stopwatch.hpp"

namespace doris {

static const int DEFAULT_DAY = 24 * 60 * 60;

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
            RETURN_IF_ERROR(
                    std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])
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

    std::tm current_date = getCurrentDate();
    int64_t diff_day = parse_diff_day_int(ctx);
    std::vector<std::string> filter_event_ids = parse_event_ids_vector(ctx);
    LOG(INFO) << "group _consumers size is: " << _consumers.size()
              << "filter_event_ids size is: " << filter_event_ids.size()
              << "diff_day is: " << diff_day;

    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer(
                    [this, consumer, capture0 = &_queue, capture1 = ctx->max_interval_s * 1000,
                     capture2 = filter_event_ids, capture3 = [this, &result_st](const Status& st) {
                         std::unique_lock<std::mutex> lock(_mutex);
                         _counter--;
                         VLOG(1) << "group counter is: " << _counter << ", grp: " << _grp_id;
                         if (_counter == 0) {
                             _queue.shutdown();
                             LOG(INFO) << "all consumers are finished. shutdown queue. group id: "
                                       << _grp_id;
                         }
                         if (result_st.ok() && !st.ok()) {
                             result_st = st;
                         }
                     }] { actual_consume(consumer, capture0, capture1, capture2, capture3); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id()
                         << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(1) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t put_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<io::PulsarConsumerPipe> pulsar_pipe =
            std::static_pointer_cast<io::PulsarConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    // copy one
    std::map<std::string, pulsar::MessageId> ack_offset = ctx->pulsar_info->ack_offset;

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
                      << ", received rows=" << received_rows << ", put rows=" << put_rows
                      << ", received bytes=" << ctx->max_batch_size - left_bytes << ", eos: " << eos
                      << ", left_time: " << left_time << ", left_bytes: " << left_bytes
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
                return Status::OK();
            }
        }

        pulsar::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            std::string partition = msg->getTopicName();
            pulsar::MessageId msg_id = msg->getMessageId();
            std::size_t len = msg->getLength();

            //filter invalid prefix of json
            std::string filter_data = substring_prefix_json(msg->getDataAsString());
            std::vector<std::string> rows = convert_rows(filter_data, current_date, diff_day);

            VLOG(3) << "get pulsar message:" << msg->getDataAsString()
                    << ", partition: " << partition << ", message id: " << msg_id
                    << ", len: " << len << ", size: " << msg->getDataAsString().size();

            Status st;
            for (std::string row : rows) {
                bool is_filter = is_filter_event_ids(row, filter_event_ids);
                if (!is_filter) {
                    continue;
                }
                size_t row_len = row.size();
                st = (pulsar_pipe.get()->*append_data)(row.c_str(), row_len);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                                 << ", row =" << row;
                    break;
                } else {
                    put_rows++;
                    left_bytes -= row_len;
                }
            }

            if (st.ok()) {
                received_rows++;
                // len of receive origin message from pulsar
                if (ack_offset[partition] >= msg_id) {
                    LOG(WARNING) << "find repeated message id: " << msg_id;
                }
                ack_offset[partition] = msg_id;
                VLOG(3) << "load message id: " << msg_id;
            } else {
                // failed to append this msg, we must stop
                LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id
                             << ", errmsg=" << st.to_string();
                eos = true;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if (result_st.ok()) {
                        result_st = st;
                    }
                }
            }
            rows.clear();
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void PulsarDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer,
                                             BlockingQueue<pulsar::Message*>* queue,
                                             int64_t max_running_time_ms,
                                             std::vector<std::string> filter_event_ids,
                                             const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->group_consume(
            queue, filter_event_ids, max_running_time_ms);
    cb(st);
}

void PulsarDataConsumerGroup::get_backlog_nums(std::shared_ptr<StreamLoadContext> ctx) {
    for (auto& consumer : _consumers) {
        // get backlog num
        int64_t backlog_num;
        Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition_backlog(
                &backlog_num);
        if (!st.ok()) {
            LOG(WARNING) << st.to_string();
        } else {
            ctx->pulsar_info
                    ->partition_backlog[std::static_pointer_cast<PulsarDataConsumer>(consumer)
                                                ->get_partition()] = backlog_num;
        }
    }
}

Status PulsarDataConsumerGroup::acknowledge_cumulative(std::shared_ptr<StreamLoadContext> ctx) {
    Status result_st = Status::OK();
    for (auto& kv : ctx->pulsar_info->ack_offset) {
        for (auto& consumer : _consumers) {
            // do cumulative ack
            Status st =
                    std::static_pointer_cast<PulsarDataConsumer>(consumer)->acknowledge_cumulative(
                            kv.second, kv.first);
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
        Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->acknowledge(message_id,
                                                                                        partition);
        if (!st.ok()) {
            LOG(WARNING) << "failed to ack message id: " << message_id
                         << ", consumer: " << consumer;
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

std::vector<std::string> PulsarDataConsumerGroup::convert_rows(std::string& data, std::tm current_date, int64_t day) {
    std::vector<std::string> targets;
    rapidjson::Document source;
    rapidjson::Document destination;
    rapidjson::StringBuffer buffer;
    //针对无法解析的json，尝试先过滤一波非utf-8的字符
    if (source.Parse(data.c_str()).HasParseError()) {
        data = removeNonUTF8Chars(data);
        source.Clear();
        rapidjson::Document().Swap(source);
    }

    if (!source.Parse(data.c_str()).HasParseError()) {
        // 根据rtime字段，日期统一转为yyyy-MM-dd格式，比对rtime和今天，判断是否在允许录入的时间范围内
        if (source.HasMember("rtime") && source["rtime"].IsString()) {
            std::string rtime = source["rtime"].GetString();
            if (!isDateInRange(rtime, current_date, day)) {
                LOG(WARNING) << "the rtime field is out of the allowed time range: " << rtime
                             << "Failed to convert rows, pass json: " << data;
                source.Clear();
                rapidjson::Document().Swap(source);
                return targets;
            }
        }

        if (source.HasMember("events") && source["events"].IsArray()) {
            const rapidjson::Value& array = source["events"];
            size_t len = array.Size();
            for (size_t i = 0; i < len; i++) {
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
                        rapidjson::Value param(rapidjson::kObjectType);
                        param.CopyFrom(object, destination.GetAllocator());
                        std::string eventStruct = convert_map_to_struct(param);
                        rapidjson::Value eventName("event", destination.GetAllocator());
                        destination.AddMember(eventName, object, destination.GetAllocator());
                        rapidjson::Value eventStructName("event_struct", destination.GetAllocator());
                        rapidjson::Value eventStructValue;
                        eventStructValue.SetString(eventStruct.c_str(),eventStruct.length(),destination.GetAllocator());
                        destination.AddMember(eventStructName, eventStructValue, destination.GetAllocator());
                    }
                }

                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                destination.Accept(writer);
                std::string dest_string(buffer.GetString(), buffer.GetSize());
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

std::string PulsarDataConsumerGroup::convert_map_to_struct(rapidjson::Value& map) {
    rapidjson::Document destination;
    destination.SetObject();
    if (map.IsObject()) {
        rapidjson::Value timeName("time", destination.GetAllocator());
        if (map.HasMember("time")) {
            rapidjson::Value& timeValue = map["time"];
            destination.AddMember(timeName, timeValue, destination.GetAllocator());
        } else {
            destination.AddMember(timeName, rapidjson::Value(), destination.GetAllocator());
        }
        rapidjson::Value lngName("lng", destination.GetAllocator());
        if (map.HasMember("lng")) {
            rapidjson::Value& lngValue = map["lng"];
            destination.AddMember(lngName, lngValue, destination.GetAllocator());
        } else {
            destination.AddMember(lngName, rapidjson::Value(), destination.GetAllocator());
        }
        rapidjson::Value latName("lat", destination.GetAllocator());
        if (map.HasMember("lat")) {
            rapidjson::Value& latValue = map["lat"];
            destination.AddMember(latName, latValue, destination.GetAllocator());
        } else {
            destination.AddMember(latName, rapidjson::Value(), destination.GetAllocator());
        }
        rapidjson::Value netName("net", destination.GetAllocator());
        if (map.HasMember("net")) {
            rapidjson::Value& netValue = map["net"];
            destination.AddMember(netName, netValue, destination.GetAllocator());
        } else {
            destination.AddMember(netName, rapidjson::Value(), destination.GetAllocator());
        }
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    destination.Accept(writer);
    std::string dest_string(buffer.GetString(), buffer.GetSize());
    buffer.Clear();
    destination.Clear();
    rapidjson::Document().Swap(destination);
    return dest_string;
}

bool PulsarDataConsumerGroup::is_filter_event_ids(
        const std::string& data, const std::vector<std::string>& filter_event_ids) {
    if (filter_event_ids.empty()) {
        return true;
    }
    for (std::string event_id : filter_event_ids) {
        if (data.find(event_id) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> PulsarDataConsumerGroup::parse_event_ids_vector(
        std::shared_ptr<StreamLoadContext> ctx) {
    std::vector<std::string> tokens;
    for (auto& item : ctx->pulsar_info->properties) {
        if (item.first == "event.ids") {
            std::stringstream ss(item.second);
            std::string token;
            while (std::getline(ss, token, ',')) {
                tokens.push_back(token);
            }
        }
    }
    return tokens;
}

int64_t PulsarDataConsumerGroup::parse_diff_day_int(std::shared_ptr<StreamLoadContext> ctx) {
    int64_t result = 3;
    for (auto& item : ctx->pulsar_info->properties) {
        if (item.first == "diff.day") {
            result = std::stoll(item.second);
        }
    }
    return result;
}

// 判断是否是UTF-8的合法字符
bool PulsarDataConsumerGroup::isValidUTF8Char(const char c) {
    if ((c & 0x80) == 0) {
        // 单字节字符
        return true;
    } else if ((c & 0xE0) == 0xC0) {
        // 双字节字符
        return true;
    } else if ((c & 0xF0) == 0xE0) {
        // 三字节字符
        return true;
    } else if ((c & 0xF8) == 0xF0) {
        // 四字节字符
        return true;
    }
    return false;
}

std::string PulsarDataConsumerGroup::removeNonUTF8Chars(const std::string& input) {
    std::string result;
    result.reserve(input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        char current_char = input[i];
        if ((current_char & 0x80) == 0) {
            // Single-byte character (ASCII), just add to the result
            result += current_char;
        } else {
            // Multi-byte character, check if it's a valid UTF-8 sequence
            int num_bytes = 1;
            while ((current_char & (0x80 >> num_bytes)) != 0) {
                ++num_bytes;
            }

            if (num_bytes == 1 || num_bytes > 4) {
                // Invalid UTF-8 sequence, skip this character
                continue;
            }

            // Check continuation bytes
            bool is_valid_UTF8 = true;
            for (int j = 1; j < num_bytes; ++j) {
                if (i + j >= input.size() || !isValidUTF8Char(input[i + j])) {
                    is_valid_UTF8 = false;
                    break;
                }
            }

            if (is_valid_UTF8) {
                // Valid UTF-8 sequence, add to the result
                result += current_char;
                for (int j = 1; j < num_bytes; ++j) {
                    result += input[i + j];
                }
                i += num_bytes - 1;
            }
        }
    }
    return result;
}

bool PulsarDataConsumerGroup::isDateInRange(std::string& date_string, std::tm current_date, int64_t day) {
    // 将字符串解析为日期
    std::tm tm = {};
    std::istringstream ss(date_string);
    ss >> std::get_time(&tm, "%Y-%m-%d");

    if (ss.fail()) {
        LOG(WARNING) << "Failed to parse the date string : " << date_string;
        return false;
    }

    // 判断是否在三天前到三天后之间（一周内）
    auto date = std::mktime(&tm);
    auto current = std::mktime(&current_date);

    return std::difftime(date, current) >= -1 * DEFAULT_DAY * day && std::difftime(date, current) <= day * DEFAULT_DAY;
}

std::tm PulsarDataConsumerGroup::getCurrentDate() {
    // 获取当前日期
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm current_date = *std::localtime(&now_time);

    // 调整当前日期到0点0分0秒
    current_date.tm_hour = 0;
    current_date.tm_min = 0;
    current_date.tm_sec = 0;

    return current_date;
}

} // namespace doris
