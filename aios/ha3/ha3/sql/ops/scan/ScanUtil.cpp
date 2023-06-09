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
#include "ha3/sql/ops/scan/ScanUtil.h"

#include "alog/Logger.h"
#include "autil/HashFuncFactory.h"
#include "autil/HashFunctionBase.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "table/Table.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace sql {

void ScanUtil::filterPksByParam(const ScanInitParam &param,
                                const suez::turing::IndexInfo *pkIndexInfo,
                                std::vector<string> &pks) {
    assert(pkIndexInfo != NULL);
    NAVI_KERNEL_LOG(TRACE3,
                    "hashFields[%s] parallelNum[%u] parallelIndex[%u] tableDist[%s]",
                    StringUtil::toString(param.hashFields).c_str(),
                    param.parallelNum, param.parallelIndex,
                    ToJsonString(param.tableDist).c_str());
    if (param.parallelNum > 1) {
        filterPksByParallel(param, pks);
    }
    filterPksByHashRange(param, pkIndexInfo, pks);
}

std::string ScanUtil::indexTypeToString(uint32_t indexType) {
    switch(indexType) {
    case it_text:
        return "text";
    case it_pack:
        return "pack";
    case it_expack:
        return "expack";
    case it_string:
        return "string";
    case it_enum:
        return "enum";
    case it_property:
        return "property";
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
        return "number";
    case it_primarykey64:
    case it_primarykey128:
        return "pk";
    case it_trie:
        return "trie";
    case it_spatial:
        return "spatial";
    case it_kv:
        return "kv";
    case it_kkv:
        return "kkv";
    case it_customized:
        return "customized";
    case it_date:
        return "date";
    case it_range:
        return "range";
    default:
        return "unknown";
    }
}


autil::HashFunctionBasePtr ScanUtil::createHashFunc(const ScanInitParam &param) {
    autil::HashFunctionBasePtr hashFuncPtr;
    string hashFunc = param.hashType;
    StringUtil::toUpperCase(hashFunc);
    // for routing hash create too slow, without filter rawpk by range
    if (hashFunc != "ROUTING_HASH") {
        hashFuncPtr = HashFuncFactory::createHashFunc(hashFunc);
        if (hashFuncPtr == nullptr) {
            NAVI_KERNEL_LOG(WARN, "create hash func[%s] failed", hashFunc.c_str());
        }
    }
    NAVI_KERNEL_LOG(TRACE3, "hash func type[%s], ptr[%p]", hashFunc.c_str(), hashFuncPtr.get());
    return hashFuncPtr;
}

void ScanUtil::filterPksByParallel(const ScanInitParam &param, std::vector<string> &pks) {
    auto it = pks.begin();
    for (size_t i = param.parallelIndex; i < pks.size(); i += param.parallelNum) {
        // ATTENTION:
        // allow swap self
        (*it++).swap(pks[i]);
    }
    NAVI_KERNEL_LOG(TRACE3,
                    "after filter by parallel, keep[%ld] erase[%ld]",
                    std::distance(pks.begin(), it),
                    std::distance(it, pks.end()));
    pks.erase(it, pks.end());
}

void ScanUtil::filterPksByHashRange(const ScanInitParam &param,
                                    const suez::turing::IndexInfo *pkIndexInfo,
                                    std::vector<string> &pks) {
    assert(pkIndexInfo != NULL);
    string hashField = param.hashFields.size() > 0 ? param.hashFields[0] : "";
    if (hashField != pkIndexInfo->fieldName) {
        NAVI_KERNEL_LOG(TRACE3, "hashField[%s] is not pk[%s], skip filter by hash range",
                        hashField.c_str(), pkIndexInfo->fieldName.c_str());
        return;
    }
    auto hashFuncPtr = createHashFunc(param);
    if (param.tableDist.partCnt == 1 || hashFuncPtr == nullptr) {
        NAVI_KERNEL_LOG(TRACE3, "partCount[%u] hashFunc[%p], skip filter by hash range",
                        param.tableDist.partCnt, hashFuncPtr.get());
        return;
    }
    auto pkFilterFunc = [&](const std::string &rawPk) {
        uint32_t hashId = hashFuncPtr->getHashId(rawPk);
        auto &range = param.scanResource.range;
        if (hashId >= range.first && hashId <= range.second) {
            return true;
        }
        return false;
    };
    auto it = std::stable_partition(pks.begin(), pks.end(), pkFilterFunc);
    NAVI_KERNEL_LOG(TRACE3, "after filter by hash range, keep[%ld] erase[%ld]",
                    std::distance(pks.begin(), it),
                    std::distance(it, pks.end()));
    pks.erase(it, pks.end());
}

std::shared_ptr<table::Table> ScanUtil::createEmptyTable(const vector<string> &outputFields, const vector<string> &outputFieldsType, const std::shared_ptr<autil::mem_pool::Pool> &pool) {
    if (!pool) {
        NAVI_KERNEL_LOG(ERROR, "pool to construct table is nullptr");
        return nullptr;
    }
    if (outputFields.size() != outputFieldsType.size()) {
        NAVI_KERNEL_LOG(ERROR, "output fields num [%zu] and output fields type num [%zu] not match", outputFields.size(), outputFieldsType.size());
        return nullptr;
    }
    table::TablePtr table = std::make_shared<table::Table>(pool);
    for (auto i = 0; i < outputFields.size(); i++) {
        auto type = ExprUtil::transSqlTypeToVariableType(outputFieldsType[i]);
        auto column = table->declareColumn(outputFields[i], type.first, type.second);
        if (!column) {
            NAVI_KERNEL_LOG(ERROR, "declare column [%s] by type [%s] failed", outputFields[i].c_str(), outputFieldsType[i].c_str());
            return nullptr;
        }
    }
    return table;
}


} // namespace sql
} // namespace isearch
