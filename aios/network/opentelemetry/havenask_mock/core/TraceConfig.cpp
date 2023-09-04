/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "aios/network/opentelemetry/core/TraceConfig.h"

namespace opentelemetry {

SamplerConfig::SamplerConfig() : rate(0), zoomOutRate(1) {}

void SamplerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("rate", rate, rate);
    json.Jsonize("zoom_out_rate", zoomOutRate, zoomOutRate);
}

bool SamplerConfig::operator==(const SamplerConfig &rhs) const {
    return rate == rhs.rate && zoomOutRate == rhs.zoomOutRate;
}

OtlpConfig::OtlpConfig() : batchSize(10), enable(true), queueSize(1000) {}

void OtlpConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("endpoint", endpoint, endpoint);
    json.Jsonize("batch_size", batchSize, batchSize);
    json.Jsonize("enable", enable, enable);
    json.Jsonize("queue_size", queueSize, queueSize);
}

bool OtlpConfig::operator==(const OtlpConfig &rhs) const {
    return endpoint == rhs.endpoint && batchSize == rhs.batchSize && enable == rhs.enable && queueSize == rhs.queueSize;
}

EagleeyeConfig::EagleeyeConfig() : enable(false), recordAll(false) {}

void EagleeyeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("log_path", logPath, logPath);
    json.Jsonize("enable", enable, enable);
    json.Jsonize("record_all", recordAll, recordAll);
}

bool EagleeyeConfig::operator==(const EagleeyeConfig &rhs) const {
    return logPath == rhs.logPath && enable == rhs.enable && recordAll == rhs.recordAll;
}

LoggerConfig::LoggerConfig() : level("WARN") {}

void LoggerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("log_path", logPath, logPath);
    json.Jsonize("level", level, level);
}

bool LoggerConfig::operator==(const LoggerConfig &rhs) const { return logPath == rhs.logPath && level == rhs.level; }

void TraceConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("eagleeye", eagleeye, eagleeye);
    json.Jsonize("otlp", otlp, otlp);
    json.Jsonize("sampler", sampler, sampler);
    json.Jsonize("logger", logger, logger);
}

bool TraceConfig::operator==(const TraceConfig &rhs) const {
    return eagleeye == eagleeye && otlp == otlp && sampler == sampler;
}

} // namespace opentelemetry
