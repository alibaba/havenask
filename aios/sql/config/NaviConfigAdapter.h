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
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "autil/Log.h"
#include "navi/config/NaviConfig.h"
#include "sql/common/Log.h" // IWYU pragma: keep

namespace sql {

class NaviConfigAdapter {
public:
    NaviConfigAdapter();
    ~NaviConfigAdapter();
    NaviConfigAdapter(const NaviConfigAdapter &) = delete;
    NaviConfigAdapter &operator=(const NaviConfigAdapter &) = delete;

public:
    bool init();
    void setPartInfo(int32_t partCount, std::vector<int32_t> partIds);
    void setGigClientConfig(const multi_call::MultiCallConfig &mcConfig,
                            const multi_call::FlowControlConfigMap &mcFlowConfigMap);
    bool createNaviConfig(const std::string &bizName, navi::NaviConfig &naviConfig);
    void setNoResource() {
        _addResource = false;
    }

public:
    static constexpr char DEFAULT_LOG_LEVEL[] = "INFO";
    static constexpr size_t DEFAULT_LOG_KEEP_COUNT = 10ul;
    static constexpr size_t DEFAULT_GRPC_PROTOCOL_THREAD_NUM = 10ul;
    static constexpr int64_t DEFAULT_GRPC_PROTOCOL_KEEP_ALIVE_INTERVAL = 5000;
    static constexpr int64_t DEFAULT_GRPC_PROTOCOL_KEEP_ALIVE_TIMEOUT = 2000;
    static constexpr int INVALID_THREAD_NUM = 0xffffffff;

private:
    bool parseConfig();
    void parseMiscConfig();
    void parseThreadPoolConfigThreadNum();
    void parseThreadPoolConfigQueueConfig();
    bool parseExtraTaskQueueConfig();
    void parseLogConfig();
    void parseGrpcConfig();
    void addLogConfig(navi::NaviConfig &naviConfig);
    bool addEngineConfig(navi::NaviConfig &naviConfig);
    bool addGigClientResourceConfig(navi::NaviConfig &naviConfig);
    bool initExtraTaskQueueFromStr(const std::string &configStr);

private:
    int32_t _partCount = -1;
    std::vector<int32_t> _partIds;
    bool _hasMcConfig = false;
    bool _addResource = true;
    multi_call::MultiCallConfig _mcConfig;
    multi_call::FlowControlConfigMap _mcFlowConfigMap;
    std::map<std::string, navi::ConcurrencyConfig> _extraTaskQueueMap;
    bool _disablePerf;
    int _threadNum;
    size_t _queueSize;
    size_t _processingSize;
    std::string _logLevel;
    size_t _logKeepCount;
    size_t _grpcProtocolThreadNum;
    int64_t _grpcProtocolKeepAliveInterval; // ms
    int64_t _grpcProtocolKeepAliveTimeout;  // ms

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
