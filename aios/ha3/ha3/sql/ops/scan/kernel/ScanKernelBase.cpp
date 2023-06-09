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
#include "ha3/sql/ops/scan/ScanKernelBase.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "rapidjson/rapidjson.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/ops/scan/KKVScan.h"
#include "ha3/sql/ops/scan/LogicalScan.h"
#include "ha3/sql/ops/scan/NormalScan.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanKernelUtil.h"
#include "ha3/sql/ops/tvf/TvfWrapper.h"
#include "ha3/sql/ops/calc/CalcWrapper.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/resource/ObjectPoolResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlConfigResource.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "suez/turing/expression/common.h"
#include "table/Table.h"

using namespace std;
using namespace table;
using namespace suez::turing;
using namespace isearch::search;
namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, ScanKernelBase);
ScanKernelBase::ScanKernelBase() {}

ScanKernelBase::~ScanKernelBase() {
    _pushDownOps.clear();
    _scanBase.reset();
}

bool ScanKernelBase::config(navi::KernelConfigContext &ctx) {
    if (!_initParam.initFromJson(ctx)) {
        return false;
    }
    KernelUtil::stripName(_initParam.outputFields);
    KernelUtil::stripName(_initParam.indexInfos);
    ScanInitParam::forbidIndex(_initParam.hintsMap, _initParam.indexInfos, _initParam.forbidIndexs);
    KernelUtil::stripName(_initParam.fieldInfos);
    KernelUtil::stripName(_initParam.usedFields);
    KernelUtil::stripName(_initParam.hashFields);

    if (ctx.hasKey("push_down_ops")) {
        auto pushDownOpsCtx = ctx.enter("push_down_ops");
        return initPushDownOpConfig(pushDownOpsCtx);
    }
    return true;
}

bool ScanKernelBase::initPushDownOpConfig(navi::KernelConfigContext &pushDownOpsCtx) {
    int32_t reserveMaxCount = -1;
    for (size_t i = 0; i < pushDownOpsCtx.size(); ++i) {
        auto opCtx = pushDownOpsCtx.enter(i);
        string opName;
        opCtx.Jsonize("op_name", opName);
        auto attrs = opCtx.enter("attrs");
        if (opName == "CalcOp") {
            if (i == 0) {
                continue;
            }
            CalcInitParam param;
            if (!param.initFromJson(attrs)) {
                return false;
            }
            _pushDownOpInitParams.push_back(any(param));
        } else if (opName == "TableFunctionScanOp") {
            TvfInitParam param;
            if (!param.initFromJson(attrs)) {
                return false;
            }
            if (reserveMaxCount < 0) {
                reserveMaxCount = param.reserveMaxCount;
            }
            _pushDownOpInitParams.push_back(any(param));
        } else if (opName == "SortOp") {
            if (i == 1) {
                continue;
            } else {
                SQL_LOG(ERROR, "push down sort op must calc and sort");
                return false;
            }
        } else {
            SQL_LOG(ERROR, "unsupport push down op: %s", opName.c_str());
            return false;
        }
    }
    if (reserveMaxCount > -1) {
        _initParam.reserveMaxCount = reserveMaxCount;
    }

    return true;
}

bool ScanKernelBase::init(navi::KernelInitContext &context,
                          const std::string &kernelName,
                          const std::string &nodeName)
{
    _initParam.opName = kernelName;
    _initParam.nodeName = nodeName;
    if (!ScanKernelUtil::convertResource(_bizResource,
                                         _queryResource,
                                         _memoryPoolResource,
                                         _naviQuerySessionR,
                                         _objectPoolResource,
                                         _metaInfoResource,
                                         _initParam.scanResource,
                                         _sqlConfigResource,
                                         _tabletManagerR)) {
        SQL_LOG(ERROR, "convert resource failed.");
        return false;
    }
    return true;
}

navi::ErrorCode ScanKernelBase::initPushDownOps() {
    for (size_t i = 0; i < _pushDownOpInitParams.size(); ++i) {
        PushDownOpPtr pushDownOpPtr;
        const string& configType =_pushDownOpInitParams[i].type().name();
        if (configType == typeid(TvfInitParam).name()) {
            pushDownOpPtr =
                createPushDownOp<TvfInitParam, TvfWrapper>(_pushDownOpInitParams[i], "tvf", to_string(i));
        } else if (configType == typeid(CalcInitParam).name()) {
            pushDownOpPtr =
                createPushDownOp<CalcInitParam, CalcWrapper>(_pushDownOpInitParams[i], "calc", to_string(i));
        } else {
            SQL_LOG(ERROR, "unsupport push down op config: %s", configType.c_str());
        }
        if (!pushDownOpPtr) {
            SQL_LOG(ERROR, "pushDownOpPtr is null")
            return navi::EC_ABORT;
        }
        if (i + 1 < _pushDownOpInitParams.size()) {
            pushDownOpPtr->setReuseTable(true);
        }
        if (!_scanBase->postInitPushDownOp(*pushDownOpPtr)) {
            SQL_LOG(ERROR, "post init push down op by scan base failed");
            return navi::EC_ABORT;
        }
        _pushDownOps.push_back(pushDownOpPtr);
    }
    return navi::EC_NONE;
}

} // namespace sql
} // namespace isearch
