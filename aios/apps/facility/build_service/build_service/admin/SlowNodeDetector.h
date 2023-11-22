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

#include <memory>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/admin/DefaultSlowNodeDetectStrategy.h"
#include "build_service/admin/NewSlowNodeDetectStrategy.h"
#include "build_service/admin/SlowNodeDetectStrategy.h"
#include "build_service/admin/SlowNodeHandleStrategy.h"
#include "build_service/admin/SlowNodeMetricReporter.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/common_define.h"
#include "build_service/config/SlowNodeDetectConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class SlowNodeDetector
{
public:
    SlowNodeDetector() : _lastDetectTime(-1), _detectInterval(0), _startTime(-1), _isTestMode(false)
    {
        std::string param = autil::EnvUtil::getEnv("SLOW_NODE_DETECT_TEST_MODE");
        bool testMode = false;
        if (param.empty() || !autil::StringUtil::fromString(param, testMode)) {
            _isTestMode = false;
        }
        _isTestMode = testMode;
    }
    ~SlowNodeDetector() {}
    SlowNodeDetector(const SlowNodeDetector&) = delete;
    SlowNodeDetector& operator=(const SlowNodeDetector&) = delete;

    template <typename Nodes>
    bool Init(const config::SlowNodeDetectConfig& config, int64_t startTime)
    {
        if (startTime == -1) {
            BS_LOG(WARN, "detector init failed. start time should not be -1.");
            return false;
        }
        _config = config;
        _startTime = startTime;
        _detectInterval = config.detectInterval * 1000000;
        _isTestMode |= config.isTestMode;
        if (_isTestMode) {
            BS_LOG(INFO, "use slowNodeDetect test mode");
        }
        if (config.slowNodeDetectStrategy == NEW_SLOW_NODE_DETECT_STRATEGY) {
            _slowNodeDetectStrategy = std::make_shared<NewSlowNodeDetectStrategy<Nodes>>(config, startTime);
        } else {
            _slowNodeDetectStrategy = std::make_shared<DefaultSlowNodeDetectStrategy<Nodes>>(config, startTime);
        }
        _slowNodeHandleStrategy = std::make_shared<SlowNodeHandleStrategy>(config, startTime);
        return true;
    }

    template <typename Nodes>
    void detectAndHandleSlowNodes(const SlowNodeMetricReporterPtr& reporter,
                                  const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, Nodes* nodes,
                                  int64_t currentTime = -1, bool simpleHandle = false);
    template <typename Nodes>
    bool isEnabled();

    int64_t getNodeStartedTime() { return _startTime; }
    SlowNodeHandleStrategy::Metric& getMetric();

private:
    int64_t _lastDetectTime;
    int64_t _detectInterval;
    int64_t _startTime;
    // only for test
    bool _isTestMode;

    SlowNodeDetectStrategyPtr _slowNodeDetectStrategy;
    SlowNodeHandleStrategyPtr _slowNodeHandleStrategy;
    SlowNodeHandleStrategy::Metric _detectMetric;
    config::SlowNodeDetectConfig _config;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SlowNodeDetector);

template <typename Nodes>
bool SlowNodeDetector::isEnabled()
{
    if (!_config.enableSlowDetect) {
        return false;
    }
    proto::RoleType roleType = proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType;
    if (roleType == proto::ROLE_PROCESSOR) {
        if (!_config.enableProcessorSlowDetect) {
            return false;
        }
    } else if (roleType == proto::ROLE_BUILDER) {
        if (!_config.enableBuilderSlowDetect) {
            return false;
        }
    } else if (roleType == proto::ROLE_MERGER) {
        if (!_config.enableMergerSlowDetect) {
            return false;
        }
    } else if (roleType == proto::ROLE_TASK) {
        if (!_config.enableTaskSlowDetect) {
            return false;
        }
    }
    return true;
}

template <typename Nodes>
inline void SlowNodeDetector::detectAndHandleSlowNodes(const SlowNodeMetricReporterPtr& reporter,
                                                       const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                                       Nodes* nodes, int64_t currentTime, bool simpleHandle)
{
    if (!isEnabled<Nodes>()) {
        return;
    }
    if (_startTime == -1) {
        return;
    }
    if (currentTime == -1) {
        currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    }
    if ((currentTime - _lastDetectTime) < _detectInterval) {
        return;
    }
    _lastDetectTime = currentTime;
    SlowNodeDetectStrategy::AbnormalNodes abnormalNodes;
    if (simpleHandle) {
        _slowNodeDetectStrategy->Detect<Nodes>(reporter, nodeStatusManager, currentTime, &abnormalNodes);
        _slowNodeHandleStrategy->simpleHandle<Nodes>(reporter, nodeStatusManager, &abnormalNodes, nodes,
                                                     &_detectMetric);
        return;
    }
    if (!_isTestMode) {
        _slowNodeDetectStrategy->Detect<Nodes>(reporter, nodeStatusManager, currentTime, &abnormalNodes);
        _slowNodeHandleStrategy->Handle<Nodes>(reporter, nodeStatusManager, &abnormalNodes, nodes, &_detectMetric);
    } else {
        abnormalNodes.slowNodes = nodeStatusManager->GetAllNodes();
        _slowNodeHandleStrategy->Handle<Nodes>(reporter, nodeStatusManager, &abnormalNodes, nodes, &_detectMetric);
    }
}

}} // namespace build_service::admin
