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
#include "sql/ops/tableModify/kernel/TableModifyKernel.h"

#include <algorithm>
#include <cstdint>
#include <engine/KernelInitContext.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/result/Result.h"
#include "kmonitor/client/MetricsReporter.h"
#include "multi_call/interface/QuerySession.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/TimeoutChecker.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/resource/QuerySessionR.h"
#include "sql/common/Log.h"
#include "sql/common/TableDistribution.h"
#include "sql/common/common.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/tableModify/AsyncMessageWriteCallbackCtx.h"
#include "sql/ops/tableModify/DeleteConditionVisitor.h"
#include "sql/ops/tableModify/MessageConstructor.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/resource/MessageWriterManager.h"
#include "suez/sdk/RemoteTableWriterParam.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace table;
using namespace autil;

namespace sql {

TableModifyKernel::TableModifyKernel() {}

TableModifyKernel::~TableModifyKernel() {}

// kernel def, see: sql/executor/proto/KernelDef.proto
void TableModifyKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.TableModifyKernel").output("output0", TableType::TYPE_ID);
}

bool TableModifyKernel::config(navi::KernelConfigContext &ctx) {
    return _initParam.initFromJson(ctx);
}

navi::ErrorCode TableModifyKernel::init(navi::KernelInitContext &ctx) {
    _beginTime = TimeUtility::currentTime();
    _tableModifyInfo.set_tablename(_initParam.tableName);
    _tableModifyInfo.set_kernelname(getKernelName());
    _tableModifyInfo.set_nodename(getNodeName());
    _kernelPool = _graphMemoryPoolR->getPool();
    initETraceId();
    if (!initMessageWriter()) {
        SQL_LOG(ERROR, "init message writer failed, table name [%s]", _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    if (!initCallbackCtx(ctx)) {
        SQL_LOG(ERROR, "init callback ctx failed, table name [%s]", _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    if (!initTableColName()) {
        SQL_LOG(
            ERROR, "init table column name failed, table name [%s]", _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    if (!startWriteMessage(autil::TimeUtility::ms2us(ctx.getTimeoutChecker()->remainTime()))) {
        SQL_LOG(ERROR, "start write message failed, table name [%s]", _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

void TableModifyKernel::initETraceId() {
    const auto &querySession = _querySessionR->getQuerySession();
    auto span = querySession->getTraceServerSpan();
    if (span) {
        _eTraceId = opentelemetry::EagleeyeUtil::getETraceId(span);
    }
}

navi::ErrorCode TableModifyKernel::compute(navi::KernelComputeContext &context) {
    uint64_t beginTime = TimeUtility::currentTime();
    table::TablePtr table;
    bool eof = true;
    bool isSucc = doCompute(table, eof);
    if (table == nullptr || !isSucc || eof) {
        reportFinalMetrics(isSucc);
        uint64_t endTime = TimeUtility::currentTime();
        _tableModifyInfo.set_totalusetime(endTime - _beginTime);
        _tableModifyInfo.set_totaloutputtime(endTime - beginTime);
        _tableModifyInfo.set_totaloutputcount(_outputCount);
        _tableModifyInfo.set_totalcomputetimes(1);
        _tableModifyInfo.set_writemsglength(_writeMsgLength);
        _tableModifyInfo.set_writemessagetime(_callbackCtx->getWriteTime());
        _sqlSearchInfoCollectorR->getCollector()->overwriteTableModifyInfo(_tableModifyInfo);
        if (table == nullptr || !isSucc) {
            SQL_LOG(WARN,
                    "modify table [%s] failed, query trace id [%s], run info: [%s]",
                    _initParam.tableName.c_str(),
                    _eTraceId.c_str(),
                    _tableModifyInfo.ShortDebugString().c_str());
            return navi::EC_ABORT;
        }
    }
    navi::PortIndex index(0, navi::INVALID_INDEX);
    TableDataPtr tableData(new TableData(table));
    context.setOutput(index, tableData, eof);
    return navi::EC_NONE;
}

bool TableModifyKernel::initCallbackCtx(navi::KernelInitContext &ctx) {
    auto asyncPipe = ctx.createAsyncPipe();
    if (!asyncPipe) {
        SQL_LOG(ERROR, "create async pipe failed");
        return false;
    }
    _callbackCtx.reset(new AsyncMessageWriteCallbackCtx(std::move(asyncPipe)));
    return true;
}

bool TableModifyKernel::initMessageWriter() {
    auto *messageWriterManager = _messageWriterManagerR->getMessageWriterManager();
    if (!messageWriterManager) {
        SQL_LOG(ERROR, "message writer manager not found");
        return false;
    }
    _messageWriter
        = messageWriterManager->getMessageWriter(_initParam.dbName, _initParam.tableName);
    if (!_messageWriter) {
        SQL_LOG(ERROR,
                "get message writer failed for zone [%s] table[%s]",
                _initParam.dbName.c_str(),
                _initParam.tableName.c_str());
        return false;
    }
    return true;
}

bool TableModifyKernel::initTableColName() {
    if (_initParam.outputFields.size() != 1) {
        SQL_LOG(ERROR,
                "invalid output fields[%s]",
                StringUtil::toString(_initParam.outputFields).c_str());
        return false;
    }
    if (_initParam.outputFieldsType.size() != 1 || _initParam.outputFieldsType[0] != "BIGINT") {
        SQL_LOG(ERROR,
                "invalid output fields type[%s]",
                StringUtil::toString(_initParam.outputFieldsType).c_str());
        return false;
    }
    _tableColName = _initParam.outputFields[0];
    return true;
}

bool TableModifyKernel::parseMessages(vector<pair<uint16_t, string>> &msgVec) {
    vector<std::map<std::string, std::string>> hashVals;
    if (!_initParam.conditionJson.empty()) {
        ConditionParser parser(_kernelPool.get());
        ConditionPtr condition;
        if (!parser.parseCondition(_initParam.conditionJson, condition)) {
            SQL_LOG(ERROR,
                    "table name [%s],  parse condition [%s] failed",
                    _initParam.tableName.c_str(),
                    _initParam.conditionJson.c_str());
            return false;
        }
        UpdateConditionVisitorPtr visitor = createUpdateConditionVisitor();
        if (visitor) {
            if (condition) {
                condition->accept(visitor.get());
            }
            if (!visitor->fetchValues(hashVals)) {
                SQL_LOG(WARN,
                        "table name [%s], fetch values failed, error info [%s]",
                        _initParam.tableName.c_str(),
                        visitor->errorInfo().c_str());
                return false;
            }
        }
    }
    if (!MessageConstructor::constructMessage(_initParam.tableDist.hashMode,
                                              _initParam.pkField,
                                              hashVals,
                                              _initParam.operation,
                                              _eTraceId,
                                              _kernelPool.get(),
                                              _initParam.modifyFieldExprsJson,
                                              msgVec)) {
        SQL_LOG(ERROR,
                "parse to message info failed, jsonStr[%s]",
                _initParam.modifyFieldExprsJson.c_str());
        return false;
    }
    return true;
}

UpdateConditionVisitorPtr TableModifyKernel::createUpdateConditionVisitor() {
    UpdateConditionVisitorPtr visitor;
    set<string> fields;
    fields.insert(_initParam.pkField);
    const auto &hashFields = _initParam.tableDist.hashMode._hashFields;
    for (auto field : hashFields) {
        fields.insert(field);
    }
    if (_initParam.operation == TABLE_OPERATION_UPDATE) {
        visitor.reset(new UpdateConditionVisitor(fields));
    } else if (_initParam.operation == TABLE_OPERATION_DELETE) {
        visitor.reset(new DeleteConditionVisitor(fields));
    }
    return visitor;
}

bool TableModifyKernel::startWriteMessage(int64_t leftTimeMs) {
    vector<pair<uint16_t, string>> msgVec;
    if (!parseMessages(msgVec)) {
        return false;
    }
    _outputCount = msgVec.size();
    _writeMsgLength = 0;
    for (const auto &msg : msgVec) {
        _writeMsgLength += msg.second.size();
    }
    int64_t timeout = leftTimeMs * 0.95;
    timeout = std::min<int64_t>(timeout, 30 * 1000 * 1000); // limit to 30s
    if (msgVec.size() > 0) {
        SQL_LOG(TRACE3,
                "write message:table[%s], range [%d], content: %s",
                _initParam.tableName.c_str(),
                msgVec[0].first,
                msgVec[0].second.c_str());
    }
    _callbackCtx->start(msgVec, _messageWriter, timeout, _querySessionR->getQuerySession());
    return true;
}

bool TableModifyKernel::doCompute(TablePtr &table, bool &eof) {
    const auto &result = _callbackCtx->getResult();
    if (result.is_err()) {
        auto message = result.get_error().message();
        SQL_LOG(ERROR,
                "write message failed, table [%s], ec[%s]",
                _initParam.tableName.c_str(),
                message.c_str());
        _tableModifyInfo.set_firstresponseinfo(message);
        return false;
    }
    table.reset(new Table(_graphMemoryPoolR->getPool()));
    auto *colRowCount = TableUtil::declareAndGetColumnData<int64_t>(table, _tableColName);
    Row row = table->allocateRow();
    colRowCount->set(row, _outputCount);
    auto &cbParam = result.get();
    _tableModifyInfo.set_maxcheckpoint(cbParam.maxCp);
    _tableModifyInfo.set_maxbuildgap(cbParam.maxBuildGap);
    _tableModifyInfo.set_firstresponseinfo(cbParam.firstResponseInfo);
    eof = true;
    SQL_LOG(TRACE2, "after do compute, table:\n%s", TableUtil::toString(table).c_str());
    return true;
}

void TableModifyKernel::reportFinalMetrics(bool isSucc) {
    bool success = _callbackCtx->tryReportMetrics(isSucc, *(_queryMetricReporterR->getReporter()));
    if (!success) {
        SQL_LOG(TRACE3, "ignore report metrics");
    }
}

REGISTER_KERNEL(TableModifyKernel);

} // namespace sql
