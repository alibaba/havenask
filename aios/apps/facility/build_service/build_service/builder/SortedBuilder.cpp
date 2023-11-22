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
#include "build_service/builder/SortedBuilder.h"

#include <cstddef>
#include <functional>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "beeper/beeper.h"
#include "build_service/common/Locator.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/util/memory_control/QuotaControl.h"
#include "kmonitor/client/MetricType.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::config;

using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::partition;

namespace build_service { namespace builder {

BS_LOG_SETUP(builder, SortedBuilder);

#define LOG_PREFIX _buildId.ShortDebugString().c_str()

SortedBuilder::SortedBuilder(const IndexBuilderPtr& indexBuilder, size_t buildTotalMemMB, fslib::fs::FileLock* fileLock,
                             const proto::BuildId& buildId)
    : Builder(indexBuilder, fileLock, buildId)
    , _running(false)
    , _buildTotalMem(buildTotalMemMB)
    , _sortQueueMem(-1)
    , _sortQueueSize(0)
{
}

SortedBuilder::~SortedBuilder() { stop(); }

bool SortedBuilder::initBuilder(const BuilderConfig& builderConfig, indexlib::util::MetricProviderPtr metricProvider)
{
    if (builderConfig.asyncBuild) {
        BS_PREFIX_LOG(INFO, "sort builder enable async build!");
        BuilderConfig newConfig = builderConfig;
        newConfig.asyncBuild = true;
        _asyncBuilder.reset(new AsyncBuilder(_indexBuilder, NULL));
        return _asyncBuilder->init(newConfig, metricProvider);
    }

    // TODO: legacy for async build shut down
    return Builder::init(builderConfig, metricProvider);
}

bool SortedBuilder::init(const BuilderConfig& builderConfig, indexlib::util::MetricProviderPtr metricProvider)
{
    initConfig(builderConfig);
    if (!initBuilder(builderConfig, metricProvider)) {
        return false;
    }

    const IndexPartitionSchemaPtr& schema = _indexBuilder->GetIndexPartition()->GetSchema();
    if (!initSortContainers(builderConfig, schema)) {
        return false;
    }

    _sortQueueSizeMetric = DECLARE_METRIC(metricProvider, "perf/sortQueueSize", kmonitor::STATUS, "count");
    _buildQueueSizeMetric = DECLARE_METRIC(metricProvider, "perf/buildQueueSize", kmonitor::STATUS, "count");
    _buildThreadStatusMetric = DECLARE_METRIC(metricProvider, "perf/buildThreadStatus", kmonitor::STATUS, "status");
    _sortThreadStatusMetric = DECLARE_METRIC(metricProvider, "perf/sortThreadStatus", kmonitor::STATUS, "status");
    _sortQueueMemUseMetric = DECLARE_METRIC(metricProvider, "basic/sortQueueMemUse", kmonitor::STATUS, "MB");

    _running = true;
    _batchBuildThreadPtr = Thread::createThread(std::bind(&SortedBuilder::buildThread, this), "BsBatchBuild");
    if (!_batchBuildThreadPtr) {
        _running = false;
        string errorMsg = "start sort build thread failed";
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool SortedBuilder::reserveMemoryQuota(const BuilderConfig& builderConfig, size_t buildTotalMemMB,
                                       const indexlib::util::QuotaControlPtr& quotaControl)
{
    int64_t sortQueueMem = calculateSortQueueMemory(builderConfig, buildTotalMemMB);
    return quotaControl->AllocateQuota(sortQueueMem * 1024 * 1024 * 2);
}

void SortedBuilder::initConfig(const BuilderConfig& builderConfig)
{
    _sortQueueMem = calculateSortQueueMemory(builderConfig, _buildTotalMem);
    _sortQueueSize = builderConfig.sortQueueSize;
    BS_PREFIX_LOG(INFO, "sortQueueMem [%ld MB], sortQueueSize [%u]", _sortQueueMem, _sortQueueSize);
}

bool SortedBuilder::initSortContainers(const BuilderConfig& builderConfig, const IndexPartitionSchemaPtr& schema)
{
    return initOneContainer(builderConfig, schema, _collectContainer) &&
           initOneContainer(builderConfig, schema, _buildContainer);
}

bool SortedBuilder::initOneContainer(const BuilderConfig& builderConfig, const IndexPartitionSchemaPtr& schema,
                                     SortDocumentContainer& container)
{
    common::Locator locator;
    if (!getLastLocator(locator)) {
        return false;
    }
    return container.init(builderConfig.sortDescriptions, schema, locator);
}

bool SortedBuilder::build(const DocumentPtr& doc)
{
    ScopedLock lock(_collectContainerLock);
    if (!_running) {
        string errorMsg = "receive document after builder stop.";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return true;
    }
    if (hasFatalError()) {
        return false;
    }

    _collectContainer.pushDocument(doc);
    REPORT_METRIC(_sortQueueSizeMetric, _collectContainer.allDocCount());
    REPORT_METRIC(_sortThreadStatusMetric, Building);
    size_t sortQueueMem = _collectContainer.memUse();
    REPORT_METRIC(_sortQueueMemUseMetric, sortQueueMem);
    if (needFlush(sortQueueMem, _collectContainer.allDocCount())) {
        BS_LOG(INFO,
               "SortedBuilder need auto flush sortQueue :"
               "sortQueueMemory[%lu] quota[%lu], queueDocCount[%lu] queueSize[%u].",
               sortQueueMem, _sortQueueMem, _collectContainer.allDocCount(), _sortQueueSize);
        _collectContainer.printMemoryInfo();
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(indexlib::INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "SortedBuilder need auto flush sortQueue :"
                                          "sortQueueMemory[%lu] quota[%lu], queueDocCount[%lu] queueSize[%u].",
                                          sortQueueMem, _sortQueueMem, _collectContainer.allDocCount(), _sortQueueSize);
        flushUnsafe();
    }
    return true;
}

void SortedBuilder::stop(int64_t stopTimestamp)
{
    BS_PREFIX_LOG(INFO, "sorted builder stop begin");
    if (!_running) {
        return;
    }

    BS_PREFIX_LOG(INFO, "flush all collected docs to build container");
    BEEPER_REPORT(indexlib::INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "SortedBuilder flush sortQueue before stop.");
    flush();

    BS_PREFIX_LOG(INFO, "wait all docs in build container built");
    waitAllDocBuilt();

    BS_PREFIX_LOG(INFO, "stop build thread");
    _running = false;
    _batchBuildThreadPtr.reset();

    if (_asyncBuilder) {
        BS_PREFIX_LOG(INFO, "stop async builder");
        _asyncBuilder->stop(stopTimestamp);
    } else {
        Builder::stop(stopTimestamp);
    }

    BS_PREFIX_LOG(INFO, "sorted builder stop end");
}

void SortedBuilder::flush()
{
    ScopedLock lock(_collectContainerLock);
    flushUnsafe();
}

void SortedBuilder::flushUnsafe()
{
    if (_collectContainer.empty()) {
        return;
    }

    int64_t beginSortTs = TimeUtility::currentTimeInSeconds();
    REPORT_METRIC(_sortThreadStatusMetric, Sorting);
    _collectContainer.sortDocument();
    REPORT_METRIC(_sortThreadStatusMetric, WaitingBuildDone);
    int64_t endSortTs = TimeUtility::currentTimeInSeconds();
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(indexlib::INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "SortedBuilder sortDocument cost [%ld] seconds, "
                                      "docCount [%ld] memoryUse [%ld]",
                                      endSortTs - beginSortTs, _collectContainer.allDocCount(),
                                      _collectContainer.memUse());

    ScopedLock lock(_swapCond);
    while (!_buildContainer.empty()) {
        _swapCond.producerWait();
    }
    _buildContainer.swap(_collectContainer);
    REPORT_METRIC(_sortQueueSizeMetric, _collectContainer.allDocCount());
    REPORT_METRIC(_buildQueueSizeMetric, _buildContainer.getUnprocessedCount());
    REPORT_METRIC(_sortQueueMemUseMetric, _collectContainer.memUse());
    _swapCond.signalConsumer();
}

void SortedBuilder::buildThread()
{
    while (_running) {
        {
            REPORT_METRIC(_buildThreadStatusMetric, WaitingSortDone);
            ScopedLock lock(_swapCond);
            while (_buildContainer.empty() && _running) {
                _swapCond.consumerWait(10 * 1000);
            }

            REPORT_METRIC(_buildThreadStatusMetric, Building);
            buildAll();
            REPORT_METRIC(_buildThreadStatusMetric, WaitingSortDone);

            _buildContainer.clear();
            _swapCond.signalProducer();
        }
        if (hasFatalError()) {
            continue;
        }

        REPORT_METRIC(_buildThreadStatusMetric, Dumping);
        dumpAll();
        REPORT_METRIC(_buildThreadStatusMetric, WaitingSortDone);
    }
}

void SortedBuilder::buildAll()
{
    int64_t beginBuildTs = TimeUtility::currentTimeInSeconds();
    for (size_t i = 0; i < _buildContainer.sortedDocCount(); ++i) {
        _buildContainer.incProcessCursor();
        REPORT_METRIC(_buildQueueSizeMetric, _buildContainer.getUnprocessedCount());
        if (!buildOneDoc(_buildContainer[i])) {
            break;
        }
    }
    int64_t endBuildTs = TimeUtility::currentTimeInSeconds();
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(indexlib::INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "SortedBuilder buildAll sortDocuments: sortedDocCount [%ld], cost [%ld] seconds.",
                                      _buildContainer.sortedDocCount(), endBuildTs - beginBuildTs);
}

void SortedBuilder::dumpAll()
{
    ServiceErrorCode ec = SERVICE_ERROR_NONE;
    string errorMsg = "sort builder dump failed:";
    while (_asyncBuilder && !_asyncBuilder->isFinished()) {
        usleep(10000); // wait 10ms
    }

    BEEPER_REPORT(indexlib::INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "SortedBuilder dump new sort segment.");
    try {
        _indexBuilder->DumpSegment();
    } catch (const ExceptionBase& e) {
        setFatalError();
        ec = BUILDER_ERROR_DUMP_FILEIO;
        errorMsg += e.what();
    } catch (...) {
        setFatalError();
        ec = BUILDER_ERROR_UNKNOWN;
        errorMsg += "unknown error";
    }
    if (ec != SERVICE_ERROR_NONE) {
        REPORT_ERROR_WITH_ADVICE(ec, errorMsg, BS_RETRY);
    }
}

void SortedBuilder::waitAllDocBuilt()
{
    while (true) {
        {
            ScopedLock lock(_swapCond);

            if (hasFatalError()) {
                break;
            }

            if (_buildContainer.empty()) {
                // TODO: legacy code for switch off async sort builder
                if (!_asyncBuilder) {
                    break;
                }
                if (_asyncBuilder->isFinished()) {
                    break;
                }
            }
        }
        usleep(10 * 1000);
    }
}

bool SortedBuilder::hasFatalError() const
{
    if (Builder::hasFatalError()) {
        return true;
    }

    if (_asyncBuilder) {
        return _asyncBuilder->hasFatalError();
    }
    return false;
}

bool SortedBuilder::tryDump()
{
    {
        ScopedLock lock(_swapCond);
        if (!_buildContainer.empty()) {
            return false;
        }
    }
    if (_indexBuilder->IsDumping()) {
        return false;
    }

    BEEPER_REPORT(indexlib::INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                  "SortedBuilder tryDump sortQueue: "
                  "memory_prefer processor needUpdateCommitedCheckpoint, "
                  "but builder read no more msg over no_more_msg_period.");
    flush();
    return true;
}

bool SortedBuilder::needFlush(size_t sortQueueMemUse, size_t docCount)
{
    return (((int64_t)sortQueueMemUse >= _sortQueueMem) || (docCount >= _sortQueueSize));
}

int64_t SortedBuilder::calculateSortQueueMemory(const BuilderConfig& builderConfig, size_t buildTotalMemMB)
{
    return builderConfig.calculateSortQueueMemory(buildTotalMemMB);
}

#undef LOG_PREFIX

}} // namespace build_service::builder
