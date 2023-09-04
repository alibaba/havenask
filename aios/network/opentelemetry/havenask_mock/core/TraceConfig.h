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
#pragma once

#include "autil/legacy/jsonizable.h"

namespace opentelemetry {

class SamplerConfig : public autil::legacy::Jsonizable {

public:
    SamplerConfig();

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const SamplerConfig &rhs) const;

public:
    // start new trace的采样率, 默认0, 1表示全采样, 0表示不采样
    uint32_t rate;
    // create childrpc的采样率, 默认1, 1表示全采样, 0表示不采样
    uint32_t zoomOutRate;
};

class OtlpConfig : public autil::legacy::Jsonizable {

public:
    OtlpConfig();

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const OtlpConfig &rhs) const;

public:
    std::string endpoint;
    size_t batchSize;
    bool enable;
    uint32_t queueSize;
};

class EagleeyeConfig : public autil::legacy::Jsonizable {

public:
    EagleeyeConfig();

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const EagleeyeConfig &rhs) const;

public:
    std::string logPath;
    bool enable;
    bool recordAll;
};

class LoggerConfig : public autil::legacy::Jsonizable {

public:
    LoggerConfig();

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const LoggerConfig &rhs) const;

public:
    std::string logPath;
    std::string level; // DISABLE | FATAL | ERROR | WARN | INFO | DEBUG | TRACE1 | TRACE2 | TRACE3
};

class TraceConfig : public autil::legacy::Jsonizable {

public:
    TraceConfig() {}

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const TraceConfig &rhs) const;

public:
    // 鹰眼的相关配置, 包括持久化配置等.
    EagleeyeConfig eagleeye;
    // Otlp的相关配置, 包括持久化配置等.
    OtlpConfig otlp;
    // 采样相关配置
    SamplerConfig sampler;
    // TraceLog相关配置
    LoggerConfig logger;
};

} // namespace opentelemetry
