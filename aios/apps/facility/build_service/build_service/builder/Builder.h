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
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "build_service/builder/BuildSpeedLimiter.h"
#include "build_service/builder/BuilderMetrics.h"
#include "build_service/common/Locator.h"
#include "build_service/common/PeriodDocCounter.h"
#include "build_service/common_define.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/Log.h"
#include "fslib/fs/FileLock.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/document.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(util, StateCounter);

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace builder {

class Builder : public proto::ErrorCollector
{
public:
    enum RET_STAT {
        RS_LOCK_BUSY,
        RS_OK,
        RS_FAIL,
    };

public:
    Builder(const indexlib::partition::IndexBuilderPtr& indexBuilder, fslib::fs::FileLock* fileLock = NULL,
            const proto::BuildId& buildId = proto::BuildId());
    virtual ~Builder();

private:
    Builder(const Builder&);
    Builder& operator=(const Builder&);

public:
    virtual bool init(const config::BuilderConfig& builderConfig,
                      indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr());
    // only return false when exception
    virtual bool build(const indexlib::document::DocumentPtr& doc);
    void flush();
    virtual bool merge(const indexlib::config::IndexPartitionOptions& options);
    virtual bool tryDump();
    virtual void stop(int64_t stopTimestamp = INVALID_TIMESTAMP);
    virtual int64_t getIncVersionTimestamp() const; // for realtime build
    virtual bool getLastLocator(common::Locator& locator) const;
    virtual bool getLatestVersionLocator(common::Locator& locator) const;
    virtual bool getLastFlushedLocator(common::Locator& locator);
    void storeBranchCkp();

    virtual void switchToConsistentMode();

    // virtual for test
    virtual bool hasFatalError() const;
    virtual const indexlib::util::CounterMapPtr& GetCounterMap()
    {
        static indexlib::util::CounterMapPtr nullCounterMap = indexlib::util::CounterMapPtr();
        return _indexBuilder ? _indexBuilder->GetCounterMap() : nullCounterMap;
    }

    void enableSpeedLimiter() { _speedLimiter.enableLimiter(); }

    void TEST_BranchFSMoveToMainRoad() { _indexBuilder->TEST_BranchFSMoveToMainRoad(); }

public:
    virtual RET_STAT getIncVersionTimestampNonBlocking(int64_t& ts) const; // for realtime build
    virtual RET_STAT getLastLocatorNonBlocking(common::Locator& locator) const;

public:
    proto::BuildStep TEST_GET_BUILD_STEP(); // for test

protected:
    // virtual for test
    virtual bool doBuild(const indexlib::document::DocumentPtr& doc);
    virtual void doMerge(const indexlib::config::IndexPartitionOptions& options);
    virtual void setFatalError();
    static std::string docOperateTypeToStr(int32_t type);

private:
    void ReportMetrics(indexlib::document::Document* doc);
    static void MaybeTraceDocType(indexlib::document::Document* doc, int32_t docOperator, int32_t originDocOperator,
                                  bool buildSuccess);
    static void MaybeTraceDocField(indexlib::document::Document* doc, fieldid_t fieldId, FieldType fieldType,
                                   common::PeriodDocCounterBase* docCounter);

protected:
    mutable autil::ThreadMutex _indexBuilderMutex;
    indexlib::partition::IndexBuilderPtr _indexBuilder;
    fslib::fs::FileLock* _fileLock; // hold this lock
    BuilderMetrics _builderMetrics;
    indexlib::util::AccumulativeCounterPtr _totalDocCountCounter;
    indexlib::util::StateCounterPtr _builderDocCountCounter;
    proto::BuildId _buildId;

private:
    volatile bool _fatalError;
    std::string _lastPk;
    BuildSpeedLimiter _speedLimiter;
    fieldid_t _fieldId;
    FieldType _fieldType;
    common::PeriodDocCounterBase* _docCounter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Builder);
}} // namespace build_service::builder
