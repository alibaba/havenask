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
#include "sql/ops/scan/ScanInitParamR.h"

#include <algorithm>
#include <cstdint>
#include <engine/NaviConfigContext.h>
#include <limits>

#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/legacy/exception.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/util/KernelUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string ScanInitParamR::RESOURCE_ID = "scan_init_param_r";

ScanInitParamR::ScanInitParamR()
    : batchSize(0)
    , limit(std::numeric_limits<uint32_t>::max())
    , parallelNum(1)
    , parallelIndex(0)
    , parallelBlockCount(PARALLEL_DEFAULT_BLOCK_COUNT)
    , opId(-1)
    , reserveMaxCount(0)
    , opScope("default") {}

ScanInitParamR::~ScanInitParamR() {}

void ScanInitParamR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool ScanInitParamR::config(navi::ResourceConfigContext &ctx) {
    try {
        NAVI_JSONIZE(ctx, PARALLEL_NUM_ATTR, parallelNum, parallelNum);
        NAVI_JSONIZE(ctx, PARALLEL_INDEX_ATTR, parallelIndex, parallelIndex);
        if (0 == parallelNum || parallelIndex >= parallelNum) {
            SQL_LOG(ERROR,
                    "illegal parallel param:parallelNum[%u],parallelIndex[%u]",
                    parallelNum,
                    parallelIndex);
            return false;
        }
        NAVI_JSONIZE(ctx, navi::NAVI_BUILDIN_ATTR_NODE, nodeName);
        NAVI_JSONIZE(ctx, navi::NAVI_BUILDIN_ATTR_KERNEL, opName);
        NAVI_JSONIZE(ctx, "table_name", tableName);
        NAVI_JSONIZE(ctx, "aux_table_name", auxTableName, auxTableName);
        NAVI_JSONIZE(ctx, "table_type", tableType);
        NAVI_JSONIZE(ctx, "db_name", dbName, dbName);
        NAVI_JSONIZE(ctx, "catalog_name", catalogName, catalogName);
        NAVI_JSONIZE(ctx, "use_nest_table", useNest, useNest);
        NAVI_JSONIZE(ctx, "nest_table_attrs", nestTableAttrs, nestTableAttrs);
        NAVI_JSONIZE(ctx, "batch_size", batchSize, batchSize);
        NAVI_JSONIZE(ctx, "limit", limit, limit);
        NAVI_JSONIZE(ctx, "hints", hintsMap, hintsMap);
        NAVI_JSONIZE(ctx, "hash_type", hashType, hashType);
        NAVI_JSONIZE(ctx, "hash_fields", hashFields, hashFields);
        NAVI_JSONIZE(ctx, "table_distribution", tableDist, tableDist);
        if (isRemoteScan(hintsMap)) {
            NAVI_JSONIZE(ctx, "location", location, location);
        }
        NAVI_JSONIZE(ctx, IQUAN_OP_ID, opId, opId);
        NAVI_JSONIZE(ctx, "table_meta", tableMeta, tableMeta);
        tableMeta.extractIndexInfos(indexInfos);
        tableMeta.extractFieldInfos(fieldInfos);
        SQL_LOG(TRACE3, "fetch index infos [%s]", autil::StringUtil::toString(indexInfos).c_str());
        SQL_LOG(TRACE3, "fetch field infos [%s]", autil::StringUtil::toString(fieldInfos).c_str());
        if (tableDist.hashMode._hashFields.size() > 0) { // compatible with old plan
            hashType = tableDist.hashMode._hashFunction;
            hashFields = tableDist.hashMode._hashFields;
        }
        // TODO
        if (tableDist.partCnt > 1 && hashFields.empty()) {
            SQL_LOG(ERROR, "hash fields is empty.");
            return false;
        }
        NAVI_JSONIZE(ctx, "used_fields", usedFields, usedFields);
        NAVI_JSONIZE(ctx, "used_fields_type", usedFieldsType, usedFieldsType);

        if (ctx.hasKey("aggregation_index_name")) {
            NAVI_JSONIZE(ctx, "aggregation_index_name", aggIndexName, aggIndexName);
            NAVI_JSONIZE(ctx, "aggregation_keys", aggKeys, aggKeys);
            NAVI_JSONIZE(ctx, "aggregation_type", aggType, aggType);
            NAVI_JSONIZE(ctx, "aggregation_value_field", aggValueField, aggValueField);
            NAVI_JSONIZE(ctx, "aggregation_distinct", aggDistinct, aggDistinct);
            NAVI_JSONIZE(ctx, "aggregation_range_map", aggRangeMap, aggRangeMap);
        }

        if (ctx.hasKey("push_down_ops")) {
            auto pushDownOpsCtx = ctx.enter("push_down_ops");
            if (!pushDownOpsCtx.isArray()) {
                SQL_LOG(ERROR, "push down ops not array.");
                return false;
            }
            if (pushDownOpsCtx.size() > 1) {
                auto lastOpCtx = pushDownOpsCtx.enter(1);
                NAVI_JSONIZE(lastOpCtx, "op_name", opName);
                if (opName == "SortOp") {
                    auto attrCtx = lastOpCtx.enter("attrs");
                    SQL_LOG(TRACE2,
                            "parse sort desc [%s]",
                            autil::StringUtil::toString(attrCtx).c_str());
                    if (!sortDesc.initFromJson(attrCtx)) {
                        SQL_LOG(ERROR,
                                "parse sort desc [%s] failed",
                                autil::StringUtil::toString(attrCtx).c_str());
                        return false;
                    }
                }
            }
        }
        NAVI_JSONIZE(ctx, "match_type", matchType, matchType);
        NAVI_JSONIZE(ctx, "op_scope", opScope, opScope);
        KernelUtil::stripName(indexInfos);
        forbidIndex(hintsMap, indexInfos, forbidIndexs);
        KernelUtil::stripName(fieldInfos);
        KernelUtil::stripName(usedFields);
        KernelUtil::stripName(hashFields);
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "scanInitParam init failed error:[%s].", e.what());
        return false;
    } catch (...) { return false; }
    return true;
}

navi::ErrorCode ScanInitParamR::init(navi::ResourceInitContext &ctx) {
    scanInfo.set_tablename(tableName);
    scanInfo.set_kernelname(opName);
    scanInfo.set_nodename(nodeName);
    scanInfo.set_parallelid(parallelIndex);
    scanInfo.set_parallelnum(parallelNum);
    if (opId < 0) {
        const std::string &keyStr
            = opName + "__" + tableName + "__" + calcInitParamR->conditionJson;
        scanInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        scanInfo.set_hashkey(opId);
    }
    patchHintInfo();
    SQL_LOG(DEBUG,
            "scan table [%s] in ip [%s], part id [%s], limit[%u].",
            tableName.c_str(),
            autil::EnvUtil::getEnv("ip", "").c_str(),
            autil::EnvUtil::getEnv("partId", "").c_str(),
            limit);
    return navi::EC_NONE;
}

const std::map<std::string, std::string> *ScanInitParamR::getScanHintMap() const {
    auto it = hintsMap.find(SQL_SCAN_HINT);
    if (it == hintsMap.end()) {
        return nullptr;
    }
    const auto &map = it->second;
    ;
    if (map.empty()) {
        return nullptr;
    }
    return &map;
}

void ScanInitParamR::patchHintInfo() {
    auto scanHintMap = getScanHintMap();
    if (!scanHintMap) {
        return;
    }
    const auto &hints = *scanHintMap;
    uint32_t localLimit = 0;
    if (fromHint(hints, "localLimit", localLimit) && localLimit > 0) {
        if (limit > 0) {
            limit = std::min(limit, localLimit);
        } else {
            limit = localLimit;
        }
    }
    uint32_t localBatchSize = 0;
    if (fromHint(hints, "batchSize", localBatchSize) && localBatchSize > 0) {
        batchSize = localBatchSize;
    }
    uint32_t parallelBlockCountFromHint = 0;
    if (fromHint(hints, PARALLEL_BLOCK_COUNT_ATTR, parallelBlockCountFromHint)
        && parallelBlockCountFromHint > 0) {
        parallelBlockCount = parallelBlockCountFromHint;
    }
}

bool ScanInitParamR::isRemoteScan(
    const std::map<std::string, std::map<std::string, std::string>> &map) {
    const auto &mapIter = map.find(SQL_SCAN_HINT);
    if (mapIter == map.end()) {
        return false;
    }
    const auto &scanHints = mapIter->second;
    return scanHints.find("remoteSourceType") != scanHints.end();
}

void ScanInitParamR::forbidIndex(
    const std::map<std::string, std::map<std::string, std::string>> &hintsMap,
    std::map<std::string, IndexInfo> &indexInfos,
    std::unordered_set<std::string> &forbidIndexs) {
    const auto &mapIter = hintsMap.find(SQL_SCAN_HINT);
    if (mapIter == hintsMap.end()) {
        return;
    }
    const std::map<std::string, std::string> &hints = mapIter->second;
    if (hints.empty()) {
        return;
    }
    const auto &iter = hints.find("forbidIndex");
    if (iter != hints.end()) {
        std::vector<std::string> indexList = autil::StringUtil::split(iter->second, ",");
        for (const auto &index : indexList) {
            indexInfos.erase(index);
            forbidIndexs.insert(index);
        }
        SQL_LOG(TRACE3,
                "after forbid index, index infos [%s]",
                autil::StringUtil::toString(indexInfos).c_str());
    }
}

void ScanInitParamR::incComputeTime() {
    scanInfo.set_totalcomputetimes(scanInfo.totalcomputetimes() + 1);
}

void ScanInitParamR::incInitTime(int64_t time) {
    scanInfo.set_totalinittime(scanInfo.totalinittime() + time);
    incTotalTime(time);
}

void ScanInitParamR::incUpdateScanQueryTime(int64_t time) {
    // TODO: use own timer
    incInitTime(time);
}

void ScanInitParamR::incSeekTime(int64_t time) {
    scanInfo.set_totalseektime(scanInfo.totalseektime() + time);
}

void ScanInitParamR::incEvaluateTime(int64_t time) {
    scanInfo.set_totalevaluatetime(scanInfo.totalevaluatetime() + time);
}

void ScanInitParamR::incOutputTime(int64_t time) {
    scanInfo.set_totaloutputtime(scanInfo.totaloutputtime() + time);
}

void ScanInitParamR::incTotalTime(int64_t time) {
    scanInfo.set_totalusetime(scanInfo.totalusetime() + time);
}

void ScanInitParamR::incTotalOutputCount(int64_t count) {
    scanInfo.set_totaloutputcount(scanInfo.totaloutputcount() + count);
}

void ScanInitParamR::setTotalScanCount(int64_t count) {
    scanInfo.set_totalscancount(count);
}

void ScanInitParamR::setTotalSeekedCount(int64_t count) {
    scanInfo.set_totalseekedcount(count);
}

void ScanInitParamR::setTotalWholeDocCount(int64_t count) {
    scanInfo.set_totalwholedoccount(count);
}

void ScanInitParamR::updateDurationTime(int64_t time) {
    scanInfo.set_durationtime(time);
}

void ScanInitParamR::updateExtraInfo(const std::string &info) {
    scanInfo.set_extrainfo(std::move(info));
}

void ScanInitParamR::incDegradedDocsCount(int64_t count) {
    scanInfo.set_degradeddocscount(scanInfo.degradeddocscount() + count);
}

REGISTER_RESOURCE(ScanInitParamR);

} // namespace sql
