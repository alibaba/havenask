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
#include "sql/ops/runSqlGraph/PartAccessAssigner.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "autil/HashAlgorithm.h"
#include "autil/HashFuncFactory.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/isearch.h"
#include "multi_call/interface/SearchService.h"
#include "multi_call/util/RandomGenerator.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/common.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/resource/GigClientR.h"
#include "rapidjson/error/en.h"
#include "sql/common/TableDistribution.h"
#include "sql/resource/HashFunctionCacheR.h"

using namespace std;
using namespace navi;
using namespace autil;
using namespace autil::legacy;

namespace sql {

AUTIL_LOG_SETUP(sql, PartAccessAssigner);

multi_call::RandomGenerator PartAccessAssigner::_randomGenerator(TimeUtility::currentTime());
static bool parseToDocument(std::string jsonStr, autil::legacy::RapidDocument &document) {
    if (jsonStr.empty()) {
        jsonStr = "{}";
    }
    jsonStr += SIMD_PADING_STR;
    document.Parse(jsonStr.c_str());
    if (document.HasParseError()) {
        NAVI_KERNEL_LOG(ERROR,
                        "invalid json, error [%s (%lu)], json: %s",
                        rapidjson::GetParseError_En(document.GetParseError()),
                        document.GetErrorOffset(),
                        jsonStr.c_str());
        return false;
    }
    return true;
}

PartAccessAssigner::PartAccessAssigner(navi::GigClientR *gigClientR,
                                       HashFunctionCacheR *hashFunctionR)
    : _gigClientR(gigClientR) // can be null if no remote sub graphs
    , _hashFunctionCacheR(hashFunctionR) {}

bool PartAccessAssigner::process(GraphDef *def) {
    size_t processed = 0;
    for (size_t i = 1; i < def->sub_graphs_size(); ++i) {
        auto *subGraphDef = def->mutable_sub_graphs(i);
        if (subGraphDef->location().biz_name() == def->sub_graphs(0).location().biz_name()) {
            // same as main graph biz
            continue;
        }
        if (!processSubGraph(subGraphDef)) {
            SQL_LOG(ERROR,
                    "process sub graph [%lu] failed, def [%s]",
                    i,
                    subGraphDef->DebugString().c_str());
            return false;
        }
        ++processed;
    }
    SQL_LOG(TRACE3, "[%lu] sub graphs processed", processed);
    return true;
}

bool PartAccessAssigner::processSubGraph(SubGraphDef *subGraphDef) {
    auto &binaryAttrsMap = *subGraphDef->mutable_binary_attrs();
    auto it = binaryAttrsMap.find("table_distribution");
    if (it == binaryAttrsMap.end()) {
        SQL_LOG(TRACE3, "table distribution not found, skip");
        return true;
    }

    RapidDocument doc;
    if (!parseToDocument(it->second, doc)) {
        SQL_LOG(ERROR, "parse table distribution json failed")
        return false;
    }

    Jsonizable::JsonWrapper json(&doc);
    TableDistribution tableDist;
    tableDist.Jsonize(json);

    const auto &bizName = subGraphDef->location().biz_name();
    GraphBuilder builder(subGraphDef);
    if (!convertTableDistToPartIds(bizName, tableDist, builder)) {
        SQL_LOG(ERROR, "convert table dist to part ids failed [%s]", it->second.c_str());
        return false;
    }
    if (!builder.ok()) {
        SQL_LOG(ERROR, "graph builder has error");
        return false;
    }
    binaryAttrsMap.erase(it);
    return true;
}

bool PartAccessAssigner::convertTableDistToPartIds(const std::string &bizName,
                                                   const TableDistribution &tableDist,
                                                   GraphBuilder &builder) {
    if (!_gigClientR) {
        SQL_LOG(ERROR,
                "gig client resource is null, can not get fullPartCount for biz [%s]",
                bizName.c_str());
        return false;
    }
    auto *searchService = _gigClientR->getSearchService().get();
    assert(searchService);
    auto fullPartCount = searchService->getBizPartCount(bizName);
    if (fullPartCount == 0) {
        SQL_LOG(ERROR, "get biz full part count failed [%s]", bizName.c_str());
        return false;
    }
    auto partIdSet = genAccessPart(fullPartCount, tableDist);
    SQL_LOG(TRACE3,
            "get part id set [%s] biz [%s] fullPartCount [%d] tableDist [%s]",
            autil::StringUtil::toString(partIdSet.begin(), partIdSet.end()).c_str(),
            bizName.c_str(),
            fullPartCount,
            FastToJsonString(tableDist, true).c_str());
    builder.partIds({partIdSet.begin(), partIdSet.end()});
    return true;
}

set<int32_t> PartAccessAssigner::genAccessPart(int32_t partCnt,
                                               const TableDistribution &tableDist) {
    if (!tableDist.debugHashValues.empty() || !tableDist.debugPartIds.empty()) {
        const auto &ret = genDebugAccessPart(partCnt, tableDist);
        SQL_LOG(DEBUG,
                "use debug part [%s] to access",
                StringUtil::toString(ret.begin(), ret.end(), ",").c_str());
        return ret;
    }
    if (tableDist.hashValues.empty() || tableDist.hashMode._hashFields.empty()) {
        if (partCnt > 1 && tableDist.partCnt == 1) {
            int32_t partId = _randomGenerator.get() % partCnt;
            SQL_LOG(INFO, "random select part id [%d] to access", partId);
            return {partId};
        } else {
            return genAllPart(partCnt);
        }
    } else {
        const string &hashField = tableDist.hashMode._hashFields[0];
        auto iter = tableDist.hashValues.find(hashField);
        if (iter == tableDist.hashValues.end()) {
            if (partCnt > 1 && tableDist.partCnt == 1) {
                int32_t partId = _randomGenerator.get() % partCnt;
                SQL_LOG(INFO, "random select part id [%d] to access", partId);
                return {partId};
            } else {
                return genAllPart(partCnt);
            }
        } else {
            if (partCnt > 1 && tableDist.partCnt == 1) {
                int32_t partId = _randomGenerator.get() % partCnt;
                SQL_LOG(INFO, "random select part id [%d] to access", partId);
                return {partId};
            }
            if (partCnt != tableDist.partCnt) {
                SQL_LOG(WARN,
                        "access part count not equal, expect [%d] actual [%d]",
                        tableDist.partCnt,
                        partCnt);
            }
            autil::HashFunctionBasePtr hashFunc = createHashFunc(tableDist.hashMode);
            if (!hashFunc) {
                return genAllPart(partCnt);
            }
            const auto &partRanges = RangeUtil::splitRange(0, MAX_PARTITION_RANGE, partCnt);
            RangeVec rangeVec;
            auto sepIter = tableDist.hashValuesSep.find(iter->first);
            string valSep = sepIter == tableDist.hashValuesSep.end() ? "" : sepIter->second;
            vector<string> hashStrVec;
            vector<string> sepStrVec;
            for (auto &hashStr : iter->second) {
                if (valSep.empty()) {
                    hashStrVec = {hashStr};
                    auto hashIds = hashFunc->getHashRange(hashStrVec);
                    rangeVec.insert(rangeVec.end(), hashIds.begin(), hashIds.end());
                } else {
                    sepStrVec.clear();
                    StringUtil::split(sepStrVec, hashStr, valSep);
                    for (auto &sepStr : sepStrVec) {
                        hashStrVec = {sepStr};
                        auto hashIds = hashFunc->getHashRange(hashStrVec);
                        rangeVec.insert(rangeVec.end(), hashIds.begin(), hashIds.end());
                    }
                }
            }
            const set<int32_t> &idSet = getPartIds(partRanges, rangeVec);
            if (idSet.empty()) {
                SQL_LOG(WARN, "gen result is empty, use full access.");
                return genAllPart(partCnt);
            } else {
                return idSet;
            }
        }
    }
}

set<int32_t> PartAccessAssigner::genDebugAccessPart(int32_t partCnt,
                                                    const TableDistribution &tableDist) {
    const string &hashValues = tableDist.debugHashValues;
    const string &partIds = tableDist.debugPartIds;
    if (!partIds.empty()) {
        set<int32_t> partSet;
        const vector<string> &idVec = StringUtil::split(partIds, ",");
        for (const auto &idStr : idVec) {
            int32_t id;
            if (StringUtil::numberFromString(idStr, id)) {
                if (id >= 0 && id < partCnt) {
                    partSet.insert(id);
                }
            }
        }
        if (partSet.empty()) {
            return genAllPart(partCnt);
        } else {
            return partSet;
        }
    }
    if (!hashValues.empty()) {
        autil::HashFunctionBasePtr hashFunc = createHashFunc(tableDist.hashMode);
        if (!hashFunc) {
            return genAllPart(partCnt);
        }
        const auto &partRanges = RangeUtil::splitRange(0, MAX_PARTITION_RANGE, partCnt);
        RangeVec rangeVec;
        const vector<string> &hashVec = StringUtil::split(hashValues, ",");
        for (const auto &hashStr : hashVec) {
            vector<string> hashStrVec = {hashStr};
            auto hashIds = hashFunc->getHashRange(hashStrVec);
            rangeVec.insert(rangeVec.end(), hashIds.begin(), hashIds.end());
        }
        const set<int32_t> &idSet = getPartIds(partRanges, rangeVec);
        if (idSet.empty()) {
            return genAllPart(partCnt);
        } else {
            return idSet;
        }
    }
    return genAllPart(partCnt);
}

set<int32_t> PartAccessAssigner::genAllPart(int32_t partCnt) {
    set<int32_t> partSet;
    for (int32_t i = 0; i < partCnt; i++) {
        partSet.insert(i);
    }
    return partSet;
}

autil::HashFunctionBasePtr PartAccessAssigner::createHashFunc(const HashMode &hashMode) {
    if (!hashMode.validate()) {
        SQL_LOG(WARN, "validate hash mode failed.");
        return HashFunctionBasePtr();
    }
    string hashFunc = hashMode._hashFunction;
    StringUtil::toUpperCase(hashFunc);
    autil::HashFunctionBasePtr hashFunctionBasePtr;
    if (_hashFunctionCacheR && hashFunc == "ROUTING_HASH") {
        uint64_t key = calHashKey(hashFunc, hashMode._hashParams);
        hashFunctionBasePtr = _hashFunctionCacheR->getHashFunc(key);
        if (hashFunctionBasePtr == nullptr) {
            hashFunctionBasePtr = HashFuncFactory::createHashFunc(
                hashFunc, hashMode._hashParams, MAX_PARTITION_COUNT);
            if (hashFunctionBasePtr != nullptr) {
                _hashFunctionCacheR->putHashFunc(key, hashFunctionBasePtr);
            }
        }
    } else {
        hashFunctionBasePtr
            = HashFuncFactory::createHashFunc(hashFunc, hashMode._hashParams, MAX_PARTITION_COUNT);
    }
    if (hashFunctionBasePtr == nullptr) {
        AUTIL_LOG(WARN, "invalid hash function [%s].", hashFunc.c_str());
    }
    return hashFunctionBasePtr;
}

set<int32_t> PartAccessAssigner::getPartIds(const RangeVec &ranges, const RangeVec &hashIds) {
    set<int32_t> partIds;
    for (size_t i = 0; i < hashIds.size(); ++i) {
        getPartId(ranges, hashIds[i], partIds);
    }
    return partIds;
}

void PartAccessAssigner::getPartId(const RangeVec &ranges,
                                   const PartitionRange &hashId,
                                   set<int32_t> &partIds) {
    for (size_t i = 0; i < ranges.size(); ++i) {
        if (!(ranges[i].second < hashId.first || ranges[i].first > hashId.second)) {
            partIds.insert(i);
        }
    }
}

uint64_t PartAccessAssigner::calHashKey(const std::string &hashName,
                                        const std::map<std::string, std::string> &hashParams) {
    string hashVal = hashName + "##";
    for (const auto &iter : hashParams) {
        hashVal += iter.first + "#_#" + iter.second + "#";
    }
    return HashAlgorithm::hashString64(hashVal.c_str(), hashVal.size());
}
} // namespace sql
