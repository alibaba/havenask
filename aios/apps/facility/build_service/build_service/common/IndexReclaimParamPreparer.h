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
#include <stdint.h>
#include <string>

#include "build_service/config/DocReclaimSource.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/metrics/Metric.h"
#include "swift/client/SwiftReader.h"

namespace build_service { namespace common {

class IndexReclaimParamPreparer
    : public indexlib::util::TaskItemWithStatus<const indexlibv2::framework::IndexTaskContext*,
                                                const std::map<std::string, std::string>&>
{
public:
    IndexReclaimParamPreparer(const std::string& clusterName);
    ~IndexReclaimParamPreparer();

public:
    indexlib::Status Run(const indexlibv2::framework::IndexTaskContext* context,
                         const std::map<std::string, std::string>& params) override;

private:
    swift::client::SwiftReader* createSwiftReader(const config::DocReclaimSource& reclaimSource,
                                                  int64_t stopTsInSecond);
    indexlib::Status generateReclaimConfigFromSwift(const config::DocReclaimSource& reclaimSource,
                                                    int64_t stopTsInSecond, std::string& content);

    // virtual for test
    virtual swift::client::SwiftReader*
    doCreateSwiftReader(const std::string& swiftRoot, const std::string& clientConfig, const std::string& readerConfig);

    std::string generationSwiftReaderConfigStr(const config::DocReclaimSource& reclaimSource);

private:
    std::string _clusterName;
    indexlib::util::MetricPtr _reclaimMsgFreshnessMetric;
    indexlib::util::MetricPtr _reclaimMsgCntMetric;
    indexlib::util::MetricPtr _reclaimMsgReadErrQpsMetric;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::common
