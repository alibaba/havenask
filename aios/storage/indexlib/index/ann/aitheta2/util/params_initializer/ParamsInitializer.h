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

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlibv2::index::ann {

class ParamsInitializer
{
public:
    ParamsInitializer() {}
    virtual ~ParamsInitializer() = default;

public:
    static bool InitAiThetaMeta(const AithetaIndexConfig& config, AiThetaMeta& meta);

public:
    virtual bool InitSearchParams(const AithetaIndexConfig& config, AiThetaParams& params);
    virtual bool InitBuildParams(const AithetaIndexConfig& config, AiThetaParams& params);
    virtual bool InitRealtimeParams(const AithetaIndexConfig& config, AiThetaParams& params);

protected:
    static bool ParseValue(const std::string& value, AiThetaParams& params, bool isOffline);
    static void UpdateCentriodCount(uint64_t docCount, const std::string& paramName, AiThetaParams& params);
    static void UpdateScanRatio(uint64_t docCount, const std::string& paramName, AiThetaParams& params,
                                uint64_t scanCount);

protected:
    static std::string CalcCentriodCount(const AiThetaParams& params, uint64_t count);
    static void UpdateAiThetaParamsKey(AiThetaParams& params);

private:
    std::string _name;

private:
    inline static const std::string _paramKeyPrefix = "proxima.";
    AUTIL_LOG_DECLARE();
};

using LrParamsInitializer = ParamsInitializer;
using ParamsInitializerPtr = std::shared_ptr<ParamsInitializer>;

} // namespace indexlibv2::index::ann
