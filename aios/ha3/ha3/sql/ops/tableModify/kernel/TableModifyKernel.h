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

#include <any>
#include <memory>

#include "autil/Log.h"
#include "ha3/sql/ops/tableModify/AsyncMessageWriteCallbackCtx.h"
#include "ha3/sql/ops/tableModify/TableModifyInitParam.h"
#include "ha3/sql/ops/tableModify/UpdateConditionVisitor.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"

namespace table {
class Table;
} // namespace table

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
class GraphMemoryPoolResource;
} // namespace navi

namespace isearch {
namespace sql {

class SqlSearchInfoCollector;
class MetaInfoResource;
class SqlQueryResource;
class MessageWriter;
class MessageWriterManagerR;

class AsyncMessageWriteCallbackCtx;

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
    bool initCallbackCtx(navi::KernelInitContext &ctx);
    bool initMessageWriter();
    bool initTableColName();
    bool parseMessages(std::vector<std::pair<uint16_t, std::string>> &msgVec);
    UpdateConditionVisitorPtr createUpdateConditionVisitor();
    bool startWriteMessage();
    bool doCompute(std::shared_ptr<table::Table> &table, bool &eof);
    void reportFinalMetrics(bool isSucc);
private:
    TableModifyInitParam _initParam;
    MessageWriterManagerR *_messageWriterManagerR = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;
    SqlSearchInfoCollector *_sqlSearchInfoCollector = nullptr;
    autil::mem_pool::PoolPtr _kernelPool;
    std::string _tableColName;
    uint64_t _beginTime = 0;
    uint32_t _outputCount = 0;
    uint32_t _writeMsgLength = 0;
    TableModifyInfo _tableModifyInfo;
    std::shared_ptr<AsyncMessageWriteCallbackCtx> _callbackCtx;
    MessageWriter *_messageWriter = nullptr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TableModifyKernel> TableModifyKernelPtr;

} // namespace sql
} // namespace isearch
