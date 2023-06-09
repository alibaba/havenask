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
#include "ha3/sql/ops/scan/ScanKernelUtil.h"

#include "alog/Logger.h"
#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlConfigResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "navi/resource/GigQuerySessionR.h"
#include "navi/resource/GraphMemoryPoolResource.h"

namespace navi {
class GraphMemoryPoolResource;
} // namespace navi

using namespace autil;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, ScanKernelUtil);

ScanKernelUtil::ScanKernelUtil() {}
ScanKernelUtil::~ScanKernelUtil() {}

bool ScanKernelUtil::convertResource(SqlBizResource *sqlBizResource,
                                     SqlQueryResource *sqlQueryResource,
                                     navi::GraphMemoryPoolResource *memoryPoolResource,
                                     navi::GigQuerySessionR *naviQuerySessionR,
                                     ObjectPoolResource *objectPoolResource,
                                     MetaInfoResource *metaInfoResource,
                                     ScanResource &scanResource,
                                     SqlConfigResource *sqlConfigResource,
                                     TabletManagerR *tabletManagerR) {
    if (!sqlBizResource) {
        SQL_LOG(ERROR, "sql biz resource is empty.");
        return false;
    }
    if (!sqlQueryResource) {
        SQL_LOG(ERROR, "sql query resource is empty.");
        return false;
    }
    scanResource.functionInterfaceCreator = sqlBizResource->getFunctionInterfaceCreator();
    scanResource.cavaPluginManager = sqlBizResource->getCavaPluginManager();
    scanResource.dependencyTableInfoMap = sqlBizResource->getDependencyTableInfoMap();
    scanResource.tableInfoWithRel = sqlBizResource->getTableInfoWithRel();
    scanResource.resourceReader = sqlBizResource->getResourceReader();
    scanResource.asyncInterExecutor = sqlBizResource->getAsyncInterExecutor();
    scanResource.asyncIntraExecutor = sqlBizResource->getAsyncIntraExecutor();
    scanResource.tableSortDescription = sqlBizResource->getTableSortDescription();

    scanResource.partId = sqlQueryResource->getPartId();
    scanResource.totalPartCount = sqlQueryResource->getTotalPartCount();
    scanResource.queryStartTime = sqlQueryResource->getStartTime();
    scanResource.timeoutMs = sqlQueryResource->getTimeoutMs();
    scanResource.queryPoolPtr = sqlQueryResource->getPoolPtr();
    scanResource.queryPool = scanResource.queryPoolPtr.get();
    scanResource.kernelPoolPtr = memoryPoolResource->getPool();
    scanResource.tracer = sqlQueryResource->getTracer();
    scanResource.searchInfoCollector = metaInfoResource ? metaInfoResource->getOverwriteInfoCollector() : nullptr;
    scanResource.metricsReporter = sqlQueryResource->getQueryMetricsReporter();
    scanResource.partitionReaderSnapshot = sqlQueryResource->getPartitionReaderSnapshot();
    scanResource.suezCavaAllocator = sqlQueryResource->getSuezCavaAllocator();
    scanResource.modelConfigMap = sqlQueryResource->getModelConfigMap();
    scanResource.memoryPoolResource = memoryPoolResource;
    scanResource.querySession = naviQuerySessionR ? naviQuerySessionR->getQuerySession() : nullptr;
    scanResource.objectPoolResource = objectPoolResource;
    scanResource.tabletManagerR = tabletManagerR;
    if (!RangeUtil::getRange(sqlQueryResource->getTotalPartCount(),
                             sqlQueryResource->getPartId(),
                             scanResource.range)) {
        SQL_LOG(ERROR,
                "get range failed, pid [%d] total count [%d]",
                sqlQueryResource->getPartId(),
                sqlQueryResource->getTotalPartCount());
        return false;
    }
    scanResource.analyzerFactory = sqlBizResource->getAnalyzerFactory();
    scanResource.queryInfo = sqlBizResource->getQueryInfo();
    scanResource.udfToQueryManager = sqlBizResource->getUdfToQueryManager();
    if (sqlConfigResource) {
        auto &sqlConfig = sqlConfigResource->getSqlConfig();
        auto &scanConfig = scanResource.scanConfig;
        scanConfig.enableScanTimeout = sqlConfig.enableScanTimeout;
        scanConfig.asyncScanConcurrency = sqlConfig.asyncScanConcurrency;
        scanConfig.targetWatermarkTimeoutRatio = sqlConfig.targetWatermarkTimeoutRatio;
        scanResource.externalTableConfig = &sqlConfig.externalTableConfig;
    }
    return true;
}

} // namespace sql
} // namespace isearch
