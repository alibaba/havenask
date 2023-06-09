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

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/turing/common/ModelConfig.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/misc/common.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace indexlib {
namespace partition {
class IndexPartitionReader;
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib

namespace multi_call {
class QuerySession;
} // namespace multi_call

namespace suez {
namespace turing {
class CavaPluginManager;
class FunctionInterfaceCreator;
class SuezCavaAllocator;
class Tracer;
} // namespace turing
} // namespace suez

using namespace suez::turing;

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace build_service {
namespace analyzer {
class AnalyzerFactory;
} // namespace analyzer
} // namespace build_service
namespace isearch {
namespace common {
class QueryInfo;
} // namespace common
namespace turing {
class Ha3SortDesc;
} // namespace turing
namespace sql {
class SqlSearchInfoCollector;
class UdfToQueryManager;
class ObjectPoolResource;
class TabletManagerR;
} // namespace sql
} // namespace isearch
namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor
namespace navi {
class GraphMemoryPoolResource;
} // namespace navi
namespace resource_reader {
class ResourceReader;
} // namespace resource_reader

namespace multi_call {
class SearchService;
}

namespace future_lite {
class Executor;
} // namespace future_lite

namespace isearch {
namespace sql {

class ExternalTableConfig;

struct ScanConfig {
    bool enableScanTimeout = true;
    size_t asyncScanConcurrency = std::numeric_limits<size_t>::max();
    double targetWatermarkTimeoutRatio = 0.5f;
};

class ScanResource {
public:
    ScanResource();
    virtual ~ScanResource();

public:
    suez::turing::TableInfoPtr getTableInfo(const std::string &tableName) const {
        if (dependencyTableInfoMap != nullptr) {
            auto tableInfo = dependencyTableInfoMap->find(tableName);
            if (tableInfo != dependencyTableInfoMap->end()) {
                return tableInfo->second;
            }
        }
        SQL_LOG(WARN, "get table info failed, tableName [%s]", tableName.c_str());
        return TableInfoPtr();
    }

    std::shared_ptr<indexlib::partition::IndexPartitionReader> getPartitionReader(const std::string &tableName) const;
    std::shared_ptr<indexlibv2::framework::ITabletReader> getTabletReader(const std::string &tableName) const;
    bool isLeader(const std::string &tableName) const;
    int64_t getBuildOffset(const std::string &tableName) const;
public:
    // from biz resource
    suez::turing::FunctionInterfaceCreator *functionInterfaceCreator = nullptr;
    suez::turing::CavaPluginManager *cavaPluginManager = nullptr;
    const std::map<std::string, suez::turing::TableInfoPtr> *dependencyTableInfoMap = nullptr;
    suez::turing::TableInfoPtr tableInfoWithRel;
    resource_reader::ResourceReader *resourceReader = nullptr;

    // from query resource
    int32_t partId = -1;
    int32_t totalPartCount = -1;
    int64_t queryStartTime = -1;
    int64_t timeoutMs = std::numeric_limits<int64_t>::max();
    autil::mem_pool::Pool *queryPool = nullptr;
    autil::mem_pool::PoolPtr queryPoolPtr;
    autil::mem_pool::PoolPtr kernelPoolPtr;
    suez::turing::Tracer *tracer = nullptr;
    SqlSearchInfoCollector *searchInfoCollector = nullptr;
    kmonitor::MetricsReporter *metricsReporter = nullptr;
    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot = nullptr;

    suez::turing::SuezCavaAllocator *suezCavaAllocator = nullptr;
    navi::GraphMemoryPoolResource *memoryPoolResource = nullptr;
    std::shared_ptr<multi_call::QuerySession> querySession;
    isearch::sql::ObjectPoolResource *objectPoolResource = nullptr;
    autil::PartitionRange range;
    const build_service::analyzer::AnalyzerFactory *analyzerFactory = nullptr;
    const common::QueryInfo *queryInfo = nullptr;
    isearch::turing::ModelConfigMap *modelConfigMap = nullptr;
    UdfToQueryManager *udfToQueryManager = nullptr;

    // for async
    future_lite::Executor *asyncInterExecutor = nullptr;
    future_lite::Executor *asyncIntraExecutor = nullptr;

    std::unordered_map<std::string, std::vector<turing::Ha3SortDesc>> *tableSortDescription = nullptr;
    ScanConfig scanConfig;
    const ExternalTableConfig *externalTableConfig = nullptr;
    TabletManagerR *tabletManagerR = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScanResource> ScanResourcePtr;
} // namespace sql
} // namespace isearch
