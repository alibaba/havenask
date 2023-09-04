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
#include <utility>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/resource/QuerySessionR.h"
#include "sql/ops/tableModify/TableModifyInitParam.h"
#include "sql/ops/tableModify/UpdateConditionVisitor.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/MessageWriterManagerR.h"
#include "sql/resource/QueryMetricReporterR.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace table {
class Table;
} // namespace table

namespace sql {

class AsyncMessageWriteCallbackCtx;
class MessageWriter;

class TableModifyKernel : public navi::Kernel {
public:
    TableModifyKernel();
    ~TableModifyKernel();

private:
    TableModifyKernel(const TableModifyKernel &);
    TableModifyKernel &operator=(const TableModifyKernel &);

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    void initETraceId();
    bool initCallbackCtx(navi::KernelInitContext &ctx);
    bool initMessageWriter();
    bool initTableColName();
    bool parseMessages(std::vector<std::pair<uint16_t, std::string>> &msgVec);
    UpdateConditionVisitorPtr createUpdateConditionVisitor();
    bool startWriteMessage(int64_t leftTimeMs);
    bool doCompute(std::shared_ptr<table::Table> &table, bool &eof);
    void reportFinalMetrics(bool isSucc);

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    KERNEL_DEPEND_ON(MessageWriterManagerR, _messageWriterManagerR);
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    KERNEL_DEPEND_ON(navi::QuerySessionR, _querySessionR);
    TableModifyInitParam _initParam;
    std::string _eTraceId;
    autil::mem_pool::PoolPtr _kernelPool;
    std::string _tableColName;
    uint64_t _beginTime = 0;
    uint32_t _outputCount = 0;
    uint32_t _writeMsgLength = 0;
    TableModifyInfo _tableModifyInfo;
    std::shared_ptr<AsyncMessageWriteCallbackCtx> _callbackCtx;
    MessageWriter *_messageWriter = nullptr;
};

typedef std::shared_ptr<TableModifyKernel> TableModifyKernelPtr;

} // namespace sql
