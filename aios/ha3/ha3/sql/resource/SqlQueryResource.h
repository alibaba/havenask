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

#include "autil/Lock.h"
#include "autil/mem_pool/Pool.h"
#include "autil/TimeoutTerminator.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "navi/common.h"
#include "navi/engine/Resource.h"

namespace navi {
class TimeoutCheckerR;
class GigQuerySessionR;
class BizKmonResource;
class GraphMemoryPoolResource;
class ResourceDefBuilder;
class ResourceInitContext;
class GigClientResource;
} // namespace navi

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;

typedef std::shared_ptr<PartitionReaderSnapshot> PartitionReaderSnapshotPtr;
} // namespace partition
} // namespace indexlib
namespace kmonitor {
class MetricsReporter;

typedef std::shared_ptr<MetricsReporter> MetricsReporterPtr;
} // namespace kmonitor

namespace isearch {
namespace turing {
class ModelConfig;

typedef std::map<std::string, ModelConfig> ModelConfigMap;
} // namespace turing
} // namespace isearch
namespace suez {
class RpcServer;
namespace turing {
class SuezCavaAllocator;
class Tracer;

typedef std::shared_ptr<Tracer> TracerPtr;
typedef std::shared_ptr<SuezCavaAllocator> SuezCavaAllocatorPtr;
} // namespace turing
} // namespace suez

namespace multi_call {
class QuerySession;

typedef std::shared_ptr<QuerySession> QuerySessionPtr;
} // namespace multi_call

namespace isearch {
namespace sql {

class TvfFuncCreatorR;
class SqlSearchInfoCollector;
class SqlBizResource;
class SqlRequestInfoR;

typedef std::shared_ptr<SqlSearchInfoCollector> SqlSearchInfoCollectorPtr;

class SqlQueryResource : public navi::Resource {
public:
    SqlQueryResource(); // to hold qrs query resource
    ~SqlQueryResource();

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

private:
    void initCommon();
    void initQrs();
    void initSearcher();
    void initSuezTuringTracerAdapter();

public:
    suez::turing::Tracer *getTracer() const;
    autil::mem_pool::Pool *getPool() const;
    autil::mem_pool::PoolPtr getPoolPtr() const;
    suez::turing::SuezCavaAllocator *getSuezCavaAllocator() const;
    autil::TimeoutTerminator *getTimeoutTerminator() const;
    indexlib::partition::PartitionReaderSnapshot *getPartitionReaderSnapshot() const;
    kmonitor::MetricsReporter *getQueryMetricsReporter() const;
    multi_call::QuerySession *getGigQuerySession() const;
    std::shared_ptr<multi_call::QuerySession> getNaviQuerySession() const;
    int64_t getStartTime() const;
    int64_t getTimeoutMs() const;
    int32_t getPartId() const;
    int32_t getTotalPartCount() const;
    isearch::turing::ModelConfigMap *getModelConfigMap() const;
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> *getTvfNameToCreator() const;
    std::string getETraceId() const;

private:
    void setPool(const autil::mem_pool::PoolPtr &queryPool);
    void setSuezCavaAllocator(const suez::turing::SuezCavaAllocatorPtr &allocator);
    void setTimeoutTerminator(const autil::TimeoutTerminatorPtr &terminator);
    void
    setPartitionReaderSnapshot(const indexlib::partition::PartitionReaderSnapshotPtr &snapshot);
    void setQueryMetricsReporter(const kmonitor::MetricsReporterPtr &reporter);
    void setGigQuerySession(multi_call::QuerySessionPtr querySession);
    void setStartTime(int64_t);
    void setTimeoutMs(int64_t timeout);
    void setPartId(int32_t partId);
    void setTotalPartCount(int32_t partCount);
    void setModelConfigMap(isearch::turing::ModelConfigMap *modelConfigMap);
    void
    setTvfNameToCreator(std::map<std::string, isearch::sql::TvfFuncCreatorR *> *tvfNameToCreator);
    void setGDBPtr(void *ptr) {
        _gdbPtr = ptr;
    }

public:
    static const std::string RESOURCE_ID;

private:
    SqlBizResource *_sqlBizResource = nullptr;
    SqlRequestInfoR *_sqlRequestInfoR = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    navi::TimeoutCheckerR *_timeoutCheckerR = nullptr;
    navi::GigQuerySessionR *_naviQuerySessionR = nullptr;
    navi::BizKmonResource *_bizKmonResource = nullptr;
    navi::GigClientResource *_gigClientResource = nullptr;
    autil::mem_pool::PoolPtr _queryPool;                             // hold ptr, it has deleter
    indexlib::partition::PartitionReaderSnapshotPtr _readerSnapshot; // hold reader
    suez::turing::SuezCavaAllocatorPtr _cavaAllocator;               // hold
    kmonitor::MetricsReporterPtr _metricsReporter;                   // hold
    autil::TimeoutTerminatorPtr _terminator;                         // hold
    suez::turing::TracerPtr _tracer;                                 // hold
    multi_call::QuerySessionPtr _querySession;                       // hold
    int64_t _startTime;
    int64_t _timeout;
    int32_t _partId;
    int32_t _totalPartCount;

    isearch::turing::ModelConfigMap *_modelConfigMap = nullptr;
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> *_tvfNameToCreator = nullptr;
    void *_gdbPtr = nullptr;
};

NAVI_TYPEDEF_PTR(SqlQueryResource);

} // namespace sql
} // namespace isearch
