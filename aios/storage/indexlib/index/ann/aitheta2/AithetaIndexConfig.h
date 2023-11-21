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
#include <string>

#include "autil/Log.h"
#include "indexlib/index/ann/aitheta2/AithetaParams.h"
#include "indexlib/util/KeyValueMap.h"
namespace indexlibv2::index::ann {

struct AithetaBuildConfig {
    std::string builderName {};
    // when doc count is smaller than buildThreshold,
    // use LinerBuilder and ignore builderName
    uint32_t buildThreshold {10000};
    bool buildInFullBuildPhase {false};
    bool storePrimaryKey {false};
    bool storeEmbedding {false};
    bool ignoreFieldCountMismatch {false};
    bool ignoreInvalidDoc {false};
    bool distributedBuild {false};
    size_t centroidCount {64};
    size_t parallelNum {8};
    std::string indexParams {"{}"};

    void Parse(const indexlib::util::KeyValueMap& parameters);
};

struct AithetaSearchConfig {
    std::string searcherName {};
    size_t scanCount {10000};
    std::string indexParams {"{}"};

    void Parse(const indexlib::util::KeyValueMap& parameters);
};

struct AithetaRealtimeConfig {
    bool enable {false};
    std::string streamerName {};
    bool storePrimaryKey {false};
    std::string indexParams {"{}"};

    void Parse(const indexlib::util::KeyValueMap& parameters);
};

struct AithetaRecallConfig {
    bool enable {false};
    float sampleRatio {0.0005f};

    void Parse(const indexlib::util::KeyValueMap& parameters);
};

struct AithetaIndexConfig {
public:
    AithetaIndexConfig(const indexlib::util::KeyValueMap& parameters);
    AithetaIndexConfig(const AithetaIndexConfig&) = default;
    AithetaIndexConfig() = default;
    ~AithetaIndexConfig() = default;

public:
    // all config
    bool ModifyConfig(const std::string& indexName, const indexlib::util::KeyValueMap& parameters);

public:
    uint32_t dimension {0};
    std::string distanceType {};
    std::string embeddingDelimiter {kEmbeddingDelimiter};

    AithetaBuildConfig buildConfig {};
    AithetaSearchConfig searchConfig {};
    AithetaRealtimeConfig realtimeConfig {};
    AithetaRecallConfig recallConfig {};

private:
    void Parse(const indexlib::util::KeyValueMap& parameters);

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<AithetaIndexConfig> AithetaIndexConfigPtr;

} // namespace indexlibv2::index::ann
