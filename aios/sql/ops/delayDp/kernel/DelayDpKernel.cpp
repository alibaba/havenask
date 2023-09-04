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
#include "sql/ops/delayDp/DelayDpKernel.h"

#include <algorithm>
#include <iosfwd>
#include <map>
#include <stdlib.h>
#include <utility>

#include "autil/DataBuffer.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "matchdoc/Trait.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/GraphDesc.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/RunGraphParams.h"
#include "navi/proto/GraphDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/runSqlGraph/PartAccessAssigner.h"
#include "sql/ops/util/KernelUtil.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace matchdoc;
using namespace navi;
using namespace autil;
using namespace table;

namespace sql {

DelayDpKernel::DelayDpKernel() {}

DelayDpKernel::~DelayDpKernel() {}

void DelayDpKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name(DELAY_DP_OP)
        .inputGroup(DEFAULT_INPUT_PORT, TableType::TYPE_ID)
        .outputGroup(DEFAULT_OUTPUT_PORT, TableType::TYPE_ID);
}

bool DelayDpKernel::config(navi::KernelConfigContext &ctx) {
    if (!initVectorAttr(ctx, DELAY_DP_INPUT_NAMES_ATTR, _inputNames)) {
        return false;
    }
    {
        vector<string> jsons;
        if (!initVectorAttr(ctx, DELAY_DP_INPUT_DISTS_ATTR, jsons)) {
            return false;
        }
        auto size = jsons.size();
        _inputDists.resize(size);
        for (size_t i = 0; i < size; ++i) {
            try {
                auto &tableDist = _inputDists[i];
                FastFromJsonString(tableDist, jsons[i]);
                if (tableDist.partCnt > 1) {
                    if (_partCnt == 1) {
                        _partCnt = tableDist.partCnt;
                    } else if (_partCnt != tableDist.partCnt) {
                        SQL_LOG(WARN,
                                "invalid table distCnd, prev:%d, cur:%d",
                                _partCnt,
                                tableDist.partCnt);
                        return false;
                    }
                }
            } catch (const legacy::ExceptionBase &e) {
                SQL_LOG(WARN,
                        "parse [%ld]th json input dist failed, error[%s]",
                        i,
                        e.GetMessage().c_str());
                return false;
            }
        }
    }
    if (!initVectorAttr(ctx, DELAY_DP_OUTPUT_NAMES_ATTR, _outputNames)) {
        return false;
    }
    if (!initGraphDef(ctx)) {
        return false;
    }
    string debugStr;
    if (initStringAttr(ctx, DELAY_DP_DEBUG_ATTR, debugStr) && debugStr == "1") {
        _debug = true;
    }
    auto inputSize = _inputNames.size();
    auto distSize = _inputDists.size();
    if (inputSize != distSize) {
        SQL_LOG(
            WARN, "input_names size[%ld] not equal to input_dists size[%ld]", inputSize, distSize);
        return false;
    }
    if (_outputNames.size() == 0u) {
        SQL_LOG(WARN, "empty output_names");
        return false;
    }

    _inputInfos.resize(distSize);
    for (size_t i = 0; i < distSize; ++i) {
        _inputInfos[i].splitUtil.init(_inputDists[i]);
    }

    return true;
}

navi::ErrorCode DelayDpKernel::init(navi::KernelInitContext &initContext) {
    return navi::EC_NONE;
}

navi::ErrorCode DelayDpKernel::compute(navi::KernelComputeContext &runContext) {
    navi::GroupDatas datas;
    if (!runContext.getGroupInput(0, datas)) {
        SQL_LOG(ERROR, "get input group failed");
        return navi::EC_ABORT;
    }
    if (!mergeInputData(datas)) {
        return navi::EC_ABORT;
    }
    if (!datas.eof()) {
        return navi::EC_NONE;
    }
    return forkGraph(runContext);
}

bool DelayDpKernel::initStringAttr(navi::KernelConfigContext &ctx,
                                   const std::string &key,
                                   std::string &value) {
    const auto &attrMap = ctx.getBinaryAttrs();
    auto iter = attrMap.find(key);
    if (iter == attrMap.end()) {
        SQL_LOG(WARN, "attr [%s] not exist", key.c_str());
        return false;
    }
    value = iter->second;
    return true;
}

bool DelayDpKernel::initVectorAttr(navi::KernelConfigContext &ctx,
                                   const std::string &key,
                                   std::vector<std::string> &values) {
    string raw;
    if (!initStringAttr(ctx, key, raw)) {
        return false;
    }
    DataBuffer buffer((void *)raw.data(), raw.size(), nullptr);
    buffer.read(values);
    return true;
}

bool DelayDpKernel::initGraphDef(navi::KernelConfigContext &ctx) {
    string raw;
    if (!initStringAttr(ctx, DELAY_DP_GRAPH_ATTR, raw)) {
        return false;
    }
    _graphDef.reset(new GraphDef());
    if (!_graphDef->ParseFromString(raw)) {
        SQL_LOG(WARN, "parse graph def failed");
        return false;
    }
    if (_graphDef->sub_graphs_size() == 0) {
        SQL_LOG(WARN, "graph def contain no sub graph");
        return false;
    }
    return true;
}

bool DelayDpKernel::mergeInputData(navi::GroupDatas &datas) {
    auto dataSize = datas.size();
    auto inputSize = _inputInfos.size();
    if (dataSize != inputSize) {
        SQL_LOG(ERROR, "group data size[%ld] not equal to config size[%ld]", dataSize, inputSize);
        return false;
    }
    for (size_t i = 0; i < dataSize; ++i) {
        auto &streamData = datas[i];
        if (streamData.data == nullptr) {
            continue;
        }
        auto inputTable = KernelUtil::getTable(streamData.data, _graphMemoryPoolR);
        if (inputTable == nullptr) {
            SQL_LOG(WARN, "invalid input table[%ld]", i);
            continue;
        }
        auto &inputInfo = _inputInfos[i];
        if (inputInfo.table == nullptr) {
            inputInfo.table = inputTable;
        } else {
            if (!inputInfo.table->merge(inputTable)) {
                SQL_LOG(WARN, "merge input table[%ld] failed", i);
                continue;
            }
            inputInfo.table->mergeDependentPools(inputTable);
        }
    }
    return true;
}

bool DelayDpKernel::splitInputData() {
    for (size_t i = 0; i < _inputInfos.size(); ++i) {
        auto &info = _inputInfos[i];
        if (info.table == nullptr) {
            SQL_LOG(ERROR, "input table[%ld] is empty", i);
            return false;
        }
        if (info.splitUtil.split(*info.table, info.partRows)) {
            _broadcast = false;
        }
    }
    return true;
}

bool DelayDpKernel::redirectGraphInput(std::vector<navi::OverrideData> &overrideDatas) {
    auto subGraph = _graphDef->mutable_sub_graphs(0);
    PartAccessAssigner util(_gigClientR, _hashFunctionCacheR);
    if (!util.processSubGraph(subGraph)) {
        SQL_LOG(WARN, "assign sub graph part failed");
        return false;
    }

    auto pbPartIds = subGraph->mutable_location()->mutable_part_info()->mutable_part_ids();
    auto partSize = pbPartIds->size();
    if (partSize == 0) {
        SQL_LOG(WARN, "no valid part");
        return false;
    }

    auto inputSize = _inputInfos.size();
    _validPartIds.reserve(partSize);
    overrideDatas.reserve(partSize * inputSize);
    for (auto partId : *pbPartIds) {
        if (!partHasData(partId)) {
            continue;
        }
        _validPartIds.push_back(partId);
        addOverrideDatas(partId, overrideDatas);
    }

    if (_validPartIds.empty()) {
        int partOffset = 0;
        if (!_debug) {
            partOffset = rand() % partSize;
        }
        auto partId = pbPartIds->Get(partOffset);
        _validPartIds.push_back(partId);
        addOverrideDatas(partId, overrideDatas);
    }

    if (_validPartIds.size() != partSize) {
        pbPartIds->Clear();
        for (auto partId : _validPartIds) {
            pbPartIds->Add(partId);
        }
    }

    return true;
}

bool DelayDpKernel::partHasData(int partId) {
    for (const auto &info : _inputInfos) {
        if (info.partRows.empty()) {
            continue;
        }
        const auto &rows = info.partRows[partId];
        if (rows.empty()) {
            return false;
        }
    }
    return true;
}

void DelayDpKernel::addOverrideDatas(int partId, std::vector<navi::OverrideData> &overrideDatas) {
    for (size_t i = 0; i < _inputInfos.size(); ++i) {
        const auto &info = _inputInfos[i];
        TablePtr table;
        if (info.partRows.empty()) {
            table = info.table;
        } else {
            table.reset(new Table(info.partRows[partId], info.table->getMatchDocAllocatorPtr()));
        }
        overrideDatas.emplace_back();
        auto &overrideData = overrideDatas.back();
        overrideData.graphId = 0;
        overrideData.partId = partId;
        overrideData.outputNode = _inputNames[i];
        overrideData.outputPort = PLACEHOLDER_OUTPUT_PORT;
        DataPtr data(new TableData(table));
        overrideData.datas = {data};
        SQL_LOG(DEBUG,
                "override data, partId:%d, row count:%ld",
                overrideData.partId,
                table->getRowCount());
        SQL_LOG(TRACE3, "table:\n%s", TableUtil::toString(table, 10).c_str());
    }
}

navi::ErrorCode DelayDpKernel::forkGraph(navi::KernelComputeContext &runContext) {
    if (!splitInputData()) {
        SQL_LOG(WARN, "split input data failed");
        return navi::EC_ABORT;
    }
    navi::ForkGraphParam param;
    if (!redirectGraphInput(param.overrideDataVec)) {
        SQL_LOG(WARN, "redirect graph input failed");
        return navi::EC_ABORT;
    }

    GraphBuilder builder(_graphDef.get());
    for (size_t i = 0; i < _outputNames.size(); ++i) {
        builder.subGraph(0)
            .node(_outputNames[i])
            .out(DEFAULT_OUTPUT_PORT)
            .toForkNodeOutput(DEFAULT_OUTPUT_PORT, i)
            .require(true);
    }
    SQL_LOG(DEBUG, "delay dp graph def:\n%s", _graphDef->ShortDebugString().c_str());
    const auto &collector = _sqlSearchInfoCollectorR->getCollector();
    DelayDpInfo info;
    info.set_kernelname(getKernelName());
    info.set_nodename(getNodeName());
    auto pbPartIds = info.mutable_partids();
    pbPartIds->Reserve(_validPartIds.size());
    for (auto id : _validPartIds) {
        pbPartIds->Add(id);
    }
    collector->overwriteDelayDpInfo(info);
    collector->setSqlRunForkGraphBeginTime(TimeUtility::currentTime());
    _sqlSearchInfoCollectorR->getCollector()->setSqlRunForkGraphBeginTime(
        TimeUtility::currentTime());
    return runContext.fork(_graphDef.release(), param);
}

REGISTER_KERNEL(DelayDpKernel);

} // namespace sql
