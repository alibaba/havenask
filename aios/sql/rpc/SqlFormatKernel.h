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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/CompressionUtil.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/ScopeTerminatorKernel.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/resource/QuerySessionR.h"
#include "navi/rpc_server/NaviArpcRequestData.h" // IWYU pragma: keep
#include "sql/framework/QrsSessionSqlResult.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/rpc/SqlRpcR.h"

namespace isearch {
namespace proto {
class QrsResponse;
} // namespace proto
} // namespace isearch
namespace navi {
class KernelComputeContext;
} // namespace navi

namespace sql {
class SqlAccessLog;
class SqlAccessLogFormatHelper;
class SqlQueryRequest;

class SqlFormatKernel : public navi::ScopeTerminatorKernel {
public:
    SqlFormatKernel();
    ~SqlFormatKernel();
    SqlFormatKernel(const SqlFormatKernel &) = delete;
    SqlFormatKernel &operator=(const SqlFormatKernel &) = delete;

public:
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &ctx) override;
    std::string getName() const override;
    std::vector<std::string> getOutputs() const override;
    navi::ErrorCode compute(navi::KernelComputeContext &ctx) override;

private:
    bool process(const autil::mem_pool::PoolPtr &pool,
                 navi::KernelComputeContext &ctx,
                 QrsSessionSqlResult &result);
    bool fillSqlPlan(SqlQueryRequest *sqlQueryRequest,
                     const navi::DataPtr &planData,
                     QrsSessionSqlResult &result) const;
    bool fillSqlResult(const navi::DataPtr &data,
                       navi::KernelComputeContext &ctx,
                       QrsSessionSqlResult &result) const;
    void fillMetaData(const navi::DataPtr &metaData,
                      navi::KernelComputeContext &ctx,
                      QrsSessionSqlResult &result);
    void formatSqlResult(SqlQueryRequest *sqlQueryRequest,
                         const SqlAccessLogFormatHelper *accessLogHelper,
                         const autil::mem_pool::PoolPtr &pool,
                         QrsSessionSqlResult &result) const;
    void fillResponse(const std::string &result,
                      autil::CompressType compressType,
                      QrsSessionSqlResult::SqlResultFormatType formatType,
                      const autil::mem_pool::PoolPtr &pool,
                      isearch::proto::QrsResponse *response);
    bool isOutputSqlPlan(sql::SqlQueryRequest *sqlQueryRequest) const;
    void initFormatType(const std::string &format);
    QrsSessionSqlResult::SearchInfoLevel
    parseSearchInfoLevel(sql::SqlQueryRequest *sqlQueryRequest) const;
    isearch::proto::FormatType
    convertFormatType(QrsSessionSqlResult::SqlResultFormatType formatType);
    void endGigTrace(size_t responseSize, QrsSessionSqlResult &result);

public:
    static const std::string KERNEL_ID;
    static const std::string INPUT_TABLE_GROUP;
    static const std::string INPUT_ERROR_GROUP;
    static const std::string OUTPUT_PORT;

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_NAMED_DATA(navi::NaviArpcRequestData, _requestData, SqlRpcR::SQL_REQUEST_NAME);
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    KERNEL_DEPEND_ON(navi::QuerySessionR, _querySessionR);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    QrsSessionSqlResult::SqlResultFormatType _formatType = QrsSessionSqlResult::SQL_RF_STRING;
    std::unique_ptr<sql::SqlAccessLog> _accessLog;
};

} // namespace sql
