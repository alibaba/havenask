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

#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/config/DocReclaimSource.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "swift/client/SwiftReader.h"

namespace build_service { namespace task_base {

class DocReclaimTask : public Task
{
public:
    DocReclaimTask();
    ~DocReclaimTask();

public:
    static const std::string TASK_NAME;

private:
    DocReclaimTask(const DocReclaimTask&);
    DocReclaimTask& operator=(const DocReclaimTask&);

public:
    bool init(TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;

    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }

private:
    swift::client::SwiftReader* createSwiftReader(const config::DocReclaimSource& reclaimSource,
                                                  int64_t stopTsInSecond);

    virtual util::SwiftClientCreator* createSwiftClientCreator();
    virtual swift::client::SwiftReader*
    doCreateSwiftReader(const std::string& swiftRoot, const std::string& clientConfig, const std::string& readerConfig);

    void removeObsoleteReclaimConfig(const std::string& reclaimConfRoot, uint32_t reservedVersionNum);
    bool generateReclaimConfigFromSwift(const std::string& reclaimSrcStr, int64_t stopTs, std::string& content);

    std::string generationSwiftReaderConfigStr(const config::DocReclaimSource& reclaimSource);
    void prepareMetrics();

private:
    mutable autil::RecursiveThreadMutex _lock;
    TaskInitParam _taskInitParam;
    config::TaskTarget _currentFinishTarget;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    std::string _reclaimConfigDir;

    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::MetricPtr _reclaimMsgFreshnessMetric;
    indexlib::util::MetricPtr _reclaimMsgCntMetric;
    indexlib::util::MetricPtr _reclaimMsgReadErrQpsMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocReclaimTask);

}} // namespace build_service::task_base
