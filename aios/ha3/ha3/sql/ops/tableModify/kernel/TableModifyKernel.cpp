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
#include "ha3/sql/ops/tableModify/kernel/TableModifyKernel.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ConditionParser.h"
#include "ha3/sql/ops/tableModify/DeleteConditionVisitor.h"
#include "ha3/sql/ops/tableModify/MessageConstructor.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/MessageWriterManager.h"
#include "ha3/sql/resource/MessageWriterManagerR.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "rapidjson/rapidjson.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace table;
using namespace autil;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, TableModifyKernel);
TableModifyKernel::TableModifyKernel() {}

TableModifyKernel::~TableModifyKernel() {}

// kernel def, see: ha3/sql/executor/proto/KernelDef.proto
void TableModifyKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("TableModifyKernel")
        .output("output0", TableType::TYPE_ID)
        .resource(MessageWriterManagerR::RESOURCE_ID, true, BIND_RESOURCE_TO(_messageWriterManagerR))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool TableModifyKernel::config(navi::KernelConfigContext &ctx) {
    return _initParam.initFromJson(ctx);
}

navi::ErrorCode TableModifyKernel::init(navi::KernelInitContext &ctx) {
    _beginTime = TimeUtility::currentTime();
    _tableModifyInfo.set_tablename(_initParam.tableName);
    _tableModifyInfo.set_kernelname(getKernelName());
    _tableModifyInfo.set_nodename(getNodeName());
    _kernelPool = _memoryPoolResource->getPool();
    _sqlSearchInfoCollector
        = _metaInfoResource ? _metaInfoResource->getOverwriteInfoCollector() : nullptr;
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
    if (!startWriteMessage()) {
        SQL_LOG(ERROR, "start write message failed, table name [%s]", _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
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
        if (_sqlSearchInfoCollector) {
            _sqlSearchInfoCollector->overwriteTableModifyInfo(_tableModifyInfo);
        }
        if (table == nullptr || !isSucc) {
            SQL_LOG(WARN,
                    "modify table [%s] failed, query trace id [%s], run info: [%s]",
                    _initParam.tableName.c_str(),
                    _queryResource->getETraceId().c_str(),
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
                                              _queryResource->getETraceId(),
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

bool TableModifyKernel::startWriteMessage() {
    vector<pair<uint16_t, string>> msgVec;
    if (!parseMessages(msgVec)) {
        return false;
    }
    _outputCount = msgVec.size();
    _writeMsgLength = 0;
    for (const auto &msg : msgVec) {
        _writeMsgLength += msg.second.size();
    }
    auto *timeoutTerminator = _queryResource->getTimeoutTerminator();
    int64_t timeout = timeoutTerminator ? timeoutTerminator->getLeftTime() * 0.95
                                        : std::numeric_limits<int64_t>::max();
    timeout = std::min<int64_t>(timeout, 30 * 1000 * 1000); // limit to 30s
    if (msgVec.size() > 0) {
        SQL_LOG(TRACE3,
                "write message:table[%s], range [%d], content: %s",
                _initParam.tableName.c_str(),
                msgVec[0].first,
                msgVec[0].second.c_str());
    }
    _callbackCtx->start(msgVec,
                        _messageWriter,
                        timeout,
                        _queryResource->getNaviQuerySession());
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
    table.reset(new Table(_memoryPoolResource->getPool()));
    auto *colRowCount = TableUtil::declareAndGetColumnData<int64_t>(table, _tableColName);
    Row row = table->allocateRow();
    colRowCount->set(row, _outputCount);
    auto &cbParam = result.get();
    _tableModifyInfo.set_maxcheckpoint(cbParam.maxCp);
    _tableModifyInfo.set_maxbuildgap(cbParam.maxBuildGap);
    _tableModifyInfo.set_firstresponseinfo(cbParam.firstResponseInfo);
    eof = true;
    SQL_LOG(TRACE3, "after do compute, table:\n%s", TableUtil::toString(table).c_str());
    return true;
}

void TableModifyKernel::reportFinalMetrics(bool isSucc) {
    auto *metricsReporter = _queryResource->getQueryMetricsReporter();
    bool success = metricsReporter && _callbackCtx->tryReportMetrics(isSucc, *metricsReporter);
    if (!success) {
        SQL_LOG(TRACE3, "ignore report metrics");
    }
}

REGISTER_KERNEL(TableModifyKernel);

} // namespace sql
} // namespace isearch
