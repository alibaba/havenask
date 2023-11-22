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
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/StringUtil.h"
#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/WorkflowThreadPool.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/MetricsReporter.h"

namespace future_lite {
class Executor;
class TaskScheduler;
} // namespace future_lite

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service {

namespace util {
class SwiftClientCreator;

typedef std::shared_ptr<SwiftClientCreator> SwiftClientCreatorPtr;
} // namespace util

namespace workflow {

struct BuildFlowThreadResource {
public:
    WorkflowThreadPoolPtr workflowThreadPool;

public:
    BuildFlowThreadResource(const WorkflowThreadPoolPtr& threadPool = WorkflowThreadPoolPtr())
        : workflowThreadPool(threadPool)
    {
    }
};

class RealtimeInfoWrapper
{
public:
    RealtimeInfoWrapper() : _valid(false) {}
    RealtimeInfoWrapper(KeyValueMap kvMap) : _kvMap(std::move(kvMap)), _valid(true) {}

public:
    bool isValid() const { return _valid; }
    // for backward index compatibility
    bool adaptsToDataLinkMode(const std::string& specifiedDataLinkMode);
    const KeyValueMap& getKvMap() const { return _kvMap; }
    indexlib::document::SrcSignature getSrcSignature() const
    {
        auto iter = _kvMap.find(config::SRC_SIGNATURE);
        uint64_t src = 0;
        if (iter != _kvMap.end() and autil::StringUtil::numberFromString(iter->second, src)) {
            return indexlib::document::SrcSignature(src);
        }
        return indexlib::document::SrcSignature();
    }

    bool load(const std::string& indexRoot);

    bool needReadRawDoc() const;
    std::string getBsServerAddress() const;
    std::string getAppName() const;
    std::string getDataTable() const;

private:
    KeyValueMap _kvMap;
    bool _valid;

private:
    BS_LOG_DECLARE();
};

struct RealtimeBuilderResource {
public:
    indexlib::util::MetricProviderPtr metricProvider;
    indexlib::util::TaskSchedulerPtr taskScheduler;
    util::SwiftClientCreatorPtr swiftClientCreator;
    std::string rawIndexRoot;
    BuildFlowThreadResource buildFlowThreadResource;
    RealtimeInfoWrapper realtimeInfo;
    future_lite::Executor* executor;
    future_lite::TaskScheduler* taskScheduler2;

public:
    RealtimeBuilderResource(kmonitor::MetricsReporterPtr reporter_, indexlib::util::TaskSchedulerPtr taskScheduler_,
                            util::SwiftClientCreatorPtr swiftClientCreator_, const std::string& rawIndexRoot_,
                            const BuildFlowThreadResource& buildFlowThreadResource_ = BuildFlowThreadResource(),
                            const RealtimeInfoWrapper& realtimeInfo_ = RealtimeInfoWrapper())
        : taskScheduler(taskScheduler_)
        , swiftClientCreator(swiftClientCreator_)
        , rawIndexRoot(rawIndexRoot_)
        , buildFlowThreadResource(buildFlowThreadResource_)
        , realtimeInfo(realtimeInfo_)
        , executor(nullptr)
        , taskScheduler2(nullptr)
    {
        metricProvider.reset(new indexlib::util::MetricProvider(reporter_));
    }

    RealtimeBuilderResource(indexlib::util::MetricProviderPtr metricProvider_,
                            indexlib::util::TaskSchedulerPtr taskScheduler_,
                            util::SwiftClientCreatorPtr swiftClientCreator_, const std::string& rawIndexRoot_,
                            const BuildFlowThreadResource& buildFlowThreadResource_ = BuildFlowThreadResource(),
                            const RealtimeInfoWrapper& realtimeInfo_ = RealtimeInfoWrapper())
        : metricProvider(metricProvider_)
        , taskScheduler(taskScheduler_)
        , swiftClientCreator(swiftClientCreator_)
        , rawIndexRoot(rawIndexRoot_)
        , buildFlowThreadResource(buildFlowThreadResource_)
        , realtimeInfo(realtimeInfo_)
        , executor(nullptr)
        , taskScheduler2(nullptr)
    {
    }
};

} // namespace workflow
} // namespace build_service
