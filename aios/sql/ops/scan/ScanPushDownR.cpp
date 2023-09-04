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
#include "sql/ops/scan/ScanPushDownR.h"

#include <algorithm>
#include <engine/NaviConfigContext.h>
#include <engine/ResourceInitContext.h>
#include <ext/alloc_traits.h>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceMap.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/ops/calc/CalcWrapperR.h"
#include "sql/ops/tvf/TvfWrapperR.h"
#include "table/Table.h"

namespace indexlib {
namespace index {
class InvertedIndexReader;
} // namespace index
} // namespace indexlib
namespace suez {
namespace turing {
class IndexInfoHelper;
class MetaInfo;
} // namespace turing
} // namespace suez

namespace sql {

const std::string ScanPushDownR::RESOURCE_ID = "scan_push_down_r";

ScanPushDownR::ScanPushDownR() {}

ScanPushDownR::~ScanPushDownR() {}

void ScanPushDownR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL)
        .enableBuildR({CalcWrapperR::RESOURCE_ID, TvfWrapperR::RESOURCE_ID});
}

bool ScanPushDownR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode ScanPushDownR::init(navi::ResourceInitContext &ctx) {
    auto configCtx = ctx.getConfigContext();
    if (!configCtx.hasKey("push_down_ops")) {
        SQL_LOG(ERROR, "lack key [push_down_ops]");
        return navi::EC_ABORT;
    }
    auto pushDownOpsCtx = configCtx.enter("push_down_ops");
    if (!initPushDownOp(ctx, pushDownOpsCtx)) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool ScanPushDownR::initPushDownOp(navi::ResourceInitContext &ctx,
                                   navi::KernelConfigContext &pushDownOpsCtx) {
    int32_t reserveMaxCount = -1;
    for (size_t i = 0; i < pushDownOpsCtx.size(); ++i) {
        auto opCtx = pushDownOpsCtx.enter(i);
        std::string opName;
        NAVI_JSONIZE(opCtx, "op_name", opName);
        auto attrs = opCtx.enter("attrs");
        if (opName == "CalcOp") {
            if (i == 0) {
                continue;
            }
            navi::ResourceMap inputResourceMap;
            auto calcWrapperR = ctx.buildR(CalcWrapperR::RESOURCE_ID, attrs, inputResourceMap);
            if (!calcWrapperR) {
                SQL_LOG(ERROR, "create CalcWrapperR failed");
                return false;
            }
            auto pushDownR = std::dynamic_pointer_cast<PushDownOp>(calcWrapperR);
            if (!pushDownR) {
                SQL_LOG(ERROR, "cast CalcWrapperR to push down failed");
                return false;
            }
            if (i + 1 < pushDownOpsCtx.size()) {
                pushDownR->setReuseTable(true);
            }
            _pushDownOps.push_back(pushDownR);
        } else if (opName == "TableFunctionScanOp") {
            navi::ResourceMap inputResourceMap;
            auto tvfWrapperR = ctx.buildR(TvfWrapperR::RESOURCE_ID, attrs, inputResourceMap);
            if (!tvfWrapperR) {
                SQL_LOG(ERROR, "create tvf failed");
                return false;
            }
            auto pushDownR = std::dynamic_pointer_cast<PushDownOp>(tvfWrapperR);
            if (!pushDownR) {
                SQL_LOG(ERROR, "cast tvf to push down failed");
                return false;
            }
            if (i + 1 < pushDownOpsCtx.size()) {
                pushDownR->setReuseTable(true);
            }
            _pushDownOps.push_back(pushDownR);
            if (reserveMaxCount < 0) {
                auto typeWrapper = dynamic_cast<TvfWrapperR *>(tvfWrapperR.get());
                if (!typeWrapper) {
                    SQL_LOG(ERROR, "cast to TvfWrapperR failed");
                }
                reserveMaxCount = typeWrapper->getInitParam().reserveMaxCount;
            }
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
        _reserveMaxCount = reserveMaxCount;
    }
    return true;
}

void ScanPushDownR::setMatchInfo(
    std::shared_ptr<suez::turing::MetaInfo> metaInfo,
    const suez::turing::IndexInfoHelper *indexInfoHelper,
    const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader) {
    for (const auto &pushDown : _pushDownOps) {
        auto *op = dynamic_cast<CalcWrapperR *>(pushDown.get());
        if (!op) {
            continue;
        }
        op->setMatchInfo(metaInfo, indexInfoHelper, indexReader);
    }
}

bool ScanPushDownR::compute(table::TablePtr &table, bool &eof) const {
    for (size_t i = 0; i < _pushDownOps.size(); ++i) {
        if (!_pushDownOps[i]->compute(table, eof)) {
            SQL_LOG(ERROR,
                    "the no.%ld push down op: %s compute failed",
                    i,
                    _pushDownOps[i]->getName().c_str());
            return false;
        }
        if (!table) {
            if (eof) {
                SQL_LOG(ERROR,
                        "the no.%ld push down op: %s output table is null",
                        i,
                        _pushDownOps[i]->getName().c_str());
                return false;
            }
            break;
        }
    }
    return true;
}

REGISTER_RESOURCE(ScanPushDownR);

} // namespace sql
