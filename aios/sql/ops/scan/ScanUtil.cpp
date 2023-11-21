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
#include "sql/ops/scan/ScanUtil.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <iterator>
#include <stdint.h>
#include <utility>

#include "autil/HashFuncFactory.h"
#include "autil/HashFunctionBase.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "navi/log/NaviLogger.h"
#include "sql/common/Log.h"
#include "sql/common/TableDistribution.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/resource/PartitionInfoR.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "table/Table.h"

using namespace std;
using namespace autil;

namespace sql {

void ScanUtil::filterPksByParam(const ScanR *scanR,
                                const ScanInitParamR &param,
                                const suez::turing::IndexInfo *pkIndexInfo,
                                std::vector<string> &pks) {
    assert(pkIndexInfo != NULL);
    SQL_LOG(DEBUG,
            "hashFields[%s] parallelNum[%u] parallelIndex[%u] tableDist[%s]",
            StringUtil::toString(param.hashFields).c_str(),
            param.parallelNum,
            param.parallelIndex,
            FastToJsonString(param.tableDist, true).c_str());
    if (param.parallelNum > 1) {
        filterPksByParallel(param, pks);
    }
    filterPksByHashRange(scanR, param, pkIndexInfo, pks);
}

autil::HashFunctionBasePtr ScanUtil::createHashFunc(const ScanInitParamR &param) {
    autil::HashFunctionBasePtr hashFuncPtr;
    string hashFunc = param.hashType;
    StringUtil::toUpperCase(hashFunc);
    // for routing hash create too slow, without filter rawpk by range
    if (hashFunc != "ROUTING_HASH") {
        hashFuncPtr = HashFuncFactory::createHashFunc(hashFunc);
        if (hashFuncPtr == nullptr) {
            SQL_LOG(WARN, "create hash func[%s] failed", hashFunc.c_str());
        }
    }
    SQL_LOG(TRACE3, "hash func type[%s], ptr[%p]", hashFunc.c_str(), hashFuncPtr.get());
    return hashFuncPtr;
}

void ScanUtil::filterPksByParallel(const ScanInitParamR &param, std::vector<string> &pks) {
    auto it = pks.begin();
    for (size_t i = param.parallelIndex; i < pks.size(); i += param.parallelNum) {
        // ATTENTION:
        // allow swap self
        (*it++).swap(pks[i]);
    }
    SQL_LOG(TRACE3,
            "after filter by parallel, keep[%ld] erase[%ld]",
            std::distance(pks.begin(), it),
            std::distance(it, pks.end()));
    pks.erase(it, pks.end());
}

void ScanUtil::filterPksByHashRange(const ScanR *scanR,
                                    const ScanInitParamR &param,
                                    const suez::turing::IndexInfo *pkIndexInfo,
                                    std::vector<string> &pks) {
    assert(pkIndexInfo != NULL);
    string hashField = param.hashFields.size() > 0 ? param.hashFields[0] : "";
    if (hashField != pkIndexInfo->fieldName) {
        SQL_LOG(TRACE3,
                "hashField[%s] is not pk[%s], skip filter by hash range",
                hashField.c_str(),
                pkIndexInfo->fieldName.c_str());
        return;
    }
    auto hashFuncPtr = createHashFunc(param);
    if (param.tableDist.partCnt == 1 || hashFuncPtr == nullptr) {
        SQL_LOG(TRACE3,
                "partCount[%u] hashFunc[%p], skip filter by hash range",
                param.tableDist.partCnt,
                hashFuncPtr.get());
        return;
    }
    auto pkFilterFunc = [&](const std::string &rawPk) {
        uint32_t hashId = hashFuncPtr->getHashId(rawPk);
        auto &range = scanR->_partitionInfoR->getRange();
        if (hashId >= range.first && hashId <= range.second) {
            return true;
        }
        return false;
    };
    auto it = std::stable_partition(pks.begin(), pks.end(), pkFilterFunc);
    SQL_LOG(TRACE3,
            "after filter by hash range, keep[%ld] erase[%ld]",
            std::distance(pks.begin(), it),
            std::distance(it, pks.end()));
    pks.erase(it, pks.end());
}

std::shared_ptr<table::Table>
ScanUtil::createEmptyTable(const vector<string> &outputFields,
                           const vector<string> &outputFieldsType,
                           const std::shared_ptr<autil::mem_pool::Pool> &pool) {
    if (!pool) {
        NAVI_KERNEL_LOG(ERROR, "pool to construct table is nullptr");
        return nullptr;
    }
    if (outputFields.size() != outputFieldsType.size()) {
        NAVI_KERNEL_LOG(ERROR,
                        "output fields num [%zu] and output fields type num [%zu] not match",
                        outputFields.size(),
                        outputFieldsType.size());
        return nullptr;
    }
    table::TablePtr table = std::make_shared<table::Table>(pool);
    for (auto i = 0; i < outputFields.size(); i++) {
        auto type = ExprUtil::transSqlTypeToVariableType(outputFieldsType[i]);
        auto column = table->declareColumn(outputFields[i], type.first, type.second);
        if (!column) {
            NAVI_KERNEL_LOG(ERROR,
                            "declare column [%s] by type [%s] failed",
                            outputFields[i].c_str(),
                            outputFieldsType[i].c_str());
            return nullptr;
        }
    }
    return table;
}

} // namespace sql
