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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/data/SqlQueryRequest.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"

namespace navi {
class GraphMemoryPoolResource;
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace isearch {
namespace sql {

class IquanResource;
class SqlConfigResource;
class MetaInfoResource;

class SqlParseKernel : public navi::Kernel {
public:
    SqlParseKernel();
    ~SqlParseKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

    static bool getOutputSqlPlan(const sql::SqlQueryRequest *sqlRequest);
private:
    bool parseSqlPlan(const sql::SqlQueryRequest *sqlRequest,
                      iquan::IquanDqlRequestPtr &iquanRequest,
                      iquan::SqlPlanPtr &sqlPlan);
    bool transToIquanRequest(const sql::SqlQueryRequest *sqlRequest,
                             iquan::IquanDqlRequest &iquanRequest);
    bool addIquanDynamicParams(const sql::SqlQueryRequest *sqlRequest,
                               iquan::DynamicParams &dynamicParams);
    bool addIquanDynamicKVParams(const sql::SqlQueryRequest *sqlRequest,
                                 iquan::DynamicParams &dynamicParams);
    void addIquanHintParams(const sql::SqlQueryRequest *sqlRequest,
                            iquan::DynamicParams &dynamicParams);
    bool addIquanSqlParams(const sql::SqlQueryRequest *sqlRequest,
                           iquan::IquanDqlRequest &iquanRequest);
    void addUserKv(const std::unordered_map<std::string, std::string> &kvPair,
                   std::map<std::string, std::string> &execParams);
    std::string getDefaultCatalogName(const sql::SqlQueryRequest *sqlRequest);
    std::string getDefaultDatabaseName(const sql::SqlQueryRequest *sqlRequest);
private:
    IquanResource *_iquanResource = nullptr;
    SqlConfigResource *_sqlConfigResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlParseKernel> SqlParseKernelPtr;
} // namespace sql
} // namespace isearch
