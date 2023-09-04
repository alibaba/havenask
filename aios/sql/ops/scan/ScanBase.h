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
#include <vector>

#include "autil/ObjectTracer.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanPushDownR.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "suez/turing/navi/QueryMemPoolR.h"

namespace autil {
class ScopedTime2;
} // namespace autil
namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
} // namespace matchdoc
namespace table {
class Table;
} // namespace table

namespace navi {
class AsyncPipe;
class ResourceInitContext;
} // namespace navi

namespace sql {

class ScanBase : public navi::Resource, public autil::ObjectTracer<ScanBase, true> {
public:
    ScanBase();
    virtual ~ScanBase();

private:
    ScanBase(const ScanBase &);
    ScanBase &operator=(const ScanBase &);

public:
    bool doInit();
    bool batchScan(std::shared_ptr<table::Table> &table, bool &eof);
    bool updateScanQuery(const StreamQueryPtr &inputQuery);
    void setPushDownMode(ScanPushDownR *scanPushDownR) {
        _scanPushDownR = scanPushDownR;
        _pushDownMode = (_scanPushDownR->size() > 0);
    }
    const std::shared_ptr<navi::AsyncPipe> &getAsyncPipe() const {
        return _asyncPipe;
    }

protected:
    std::shared_ptr<table::Table>
    createTable(std::vector<matchdoc::MatchDoc> &matchDocVec,
                const std::shared_ptr<matchdoc::MatchDocAllocator> &matchDocAllocator,
                bool reuseMatchDocAllocator);
    bool initAsyncPipe(navi::ResourceInitContext &ctx);
    void setAsyncPipe(const std::shared_ptr<navi::AsyncPipe> &asyncPipe) {
        _asyncPipe = asyncPipe;
    }

private:
    virtual bool doBatchScan(std::shared_ptr<table::Table> &table, bool &eof) {
        return false;
    }
    virtual bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
        return false;
    }
    virtual std::shared_ptr<matchdoc::MatchDocAllocator>
    copyMatchDocAllocator(std::vector<matchdoc::MatchDoc> &matchDocVec,
                          const std::shared_ptr<matchdoc::MatchDocAllocator> &matchDocAllocator,
                          bool reuseMatchDocAllocator,
                          std::vector<matchdoc::MatchDoc> &copyMatchDocs);
    virtual std::shared_ptr<table::Table>
    doCreateTable(std::shared_ptr<matchdoc::MatchDocAllocator> outputAllocator,
                  std::vector<matchdoc::MatchDoc> copyMatchDocs);

    void reportBaseMetrics();
    virtual void onBatchScanFinish() {}
    virtual void reportFinishMetrics() {}

protected:
    virtual std::string getTableNameForMetrics() const {
        return _scanInitParamR->tableName;
    }

private:
    std::unique_ptr<autil::ScopedTime2> _durationTimer;

private:
    RESOURCE_DEPEND_DECLARE();

protected:
    RESOURCE_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    RESOURCE_DEPEND_ON(ScanInitParamR, _scanInitParamR);
    RESOURCE_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    RESOURCE_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    RESOURCE_DEPEND_ON(suez::turing::QueryMemPoolR, _queryMemPoolR);
    RESOURCE_DEPEND_ON(TimeoutTerminatorR, _timeoutTerminatorR);
    // optional
    bool _scanOnce;
    bool _pushDownMode;
    uint32_t _batchSize;
    uint32_t _limit;
    uint32_t _seekCount; // statistics
    // for lifetime
    InnerScanInfo _innerScanInfo;
    std::string _tableMeta;
    ScanPushDownR *_scanPushDownR = nullptr;
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
};

typedef std::shared_ptr<ScanBase> ScanBasePtr;
} // namespace sql
