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

#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/QuerySessionR.h"
#include "navi/rpc_server/NaviArpcRequestData.h" // IWYU pragma: keep
#include "sql/common/SqlAuthManagerR.h"
#include "sql/rpc/SqlRpcR.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace sql {

class SqlQueryRequest;

class SqlRpcKernel : public navi::Kernel {
public:
    SqlRpcKernel();
    ~SqlRpcKernel();
    SqlRpcKernel(const SqlRpcKernel &) = delete;
    SqlRpcKernel &operator=(const SqlRpcKernel &) = delete;

public:
    static const std::string KERNEL_ID;
    static const std::string OUTPUT_PORT;
    static const std::string OUTPUT_PORT2;

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &ctx) override;
    navi::ErrorCode compute(navi::KernelComputeContext &ctx) override;

private:
    bool checkAuth(const std::shared_ptr<SqlQueryRequest> &sqlRequest) const;

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_NAMED_DATA(navi::NaviArpcRequestData, _requestData, SqlRpcR::SQL_REQUEST_NAME);
    KERNEL_DEPEND_ON(navi::QuerySessionR, _querySessionR);
    KERNEL_DEPEND_ON(SqlAuthManagerR, _sqlAuthManagerR);
};

} // namespace sql
