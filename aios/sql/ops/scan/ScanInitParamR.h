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

#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "ha3/common/Query.h"
#include "iquan/common/catalog/LocationDef.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/common/FieldInfo.h"
#include "sql/common/IndexInfo.h"
#include "sql/common/TableDistribution.h"
#include "sql/common/TableMeta.h"
#include "sql/common/WatermarkType.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/sort/SortInitParam.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "suez/turing/navi/TableInfoR.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

struct StreamQuery {
    isearch::common::QueryPtr query;
    std::vector<std::string> primaryKeys;
    std::string toString() {
        if (query) {
            return query->toString();
        }
        if (!primaryKeys.empty()) {
            std::stringstream ss;
            ss << "primaryKeys:[";
            for (size_t i = 0; i < primaryKeys.size(); ++i) {
                ss << (i == 0 ? "" : ", ") << primaryKeys[i];
            }
            ss << "]";
            return ss.str();
        }
        return "";
    }
};
typedef std::shared_ptr<StreamQuery> StreamQueryPtr;

struct NestTableAttr : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("output_fields", outputFields, outputFields);
        json.Jsonize("output_fields_type", outputFieldsType, outputFieldsType);
        json.Jsonize("table_name", tableName, tableName);
    }
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    std::string tableName;
};

class ScanInitParamR : public navi::Resource {
public:
    ScanInitParamR();
    ~ScanInitParamR();
    ScanInitParamR(const ScanInitParamR &) = delete;
    ScanInitParamR &operator=(const ScanInitParamR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

public:
    static bool
    isRemoteScan(const std::map<std::string, std::map<std::string, std::string>> &hintsMap);
    static void
    forbidIndex(const std::map<std::string, std::map<std::string, std::string>> &hintsMap,
                std::map<std::string, IndexInfo> &indexInfos,
                std::unordered_set<std::string> &forbidIndexs);

public:
    template <typename T>
    static bool
    fromHint(const std::map<std::string, std::string> &hints, const std::string &key, T &val) {
        auto it = hints.find(key);
        if (it == hints.end()) {
            return false;
        }
        return autil::StringUtil::fromString(it->second, val);
    }
    void patchHintInfo();
    const std::map<std::string, std::string> *getScanHintMap() const;

public:
    void incComputeTime();
    void incInitTime(int64_t time);
    void incUpdateScanQueryTime(int64_t time);
    void incSeekTime(int64_t time);
    void incEvaluateTime(int64_t time);
    void incOutputTime(int64_t time);
    void incTotalTime(int64_t time);
    void incTotalScanCount(int64_t count);
    void incTotalSeekCount(int64_t count);
    void incTotalOutputCount(int64_t count);
    void setTotalSeekCount(int64_t count);
    void updateDurationTime(int64_t time);
    void updateExtraInfo(const std::string &info);
    void incDegradedDocsCount(int64_t count);

private:
    RESOURCE_DEPEND_DECLARE();

public:
    RESOURCE_DEPEND_ON(suez::turing::TableInfoR, tableInfoR);
    RESOURCE_DEPEND_ON(CalcInitParamR, calcInitParamR);
    std::map<std::string, std::map<std::string, std::string>> hintsMap;
    std::map<std::string, IndexInfo> indexInfos;
    std::map<std::string, FieldInfo> fieldInfos;
    std::vector<std::string> hashFields;
    std::vector<std::string> usedFields;
    TableMeta tableMeta;
    std::string opName;
    std::string nodeName;
    std::string tableName;
    std::string auxTableName;
    std::string dbName;
    std::string catalogName;
    std::string tableType;
    std::string hashType;
    iquan::LocationDef location;
    TableDistribution tableDist;
    int64_t targetWatermark;
    WatermarkType targetWatermarkType;
    uint32_t batchSize;
    uint32_t limit;
    uint32_t parallelNum;
    uint32_t parallelIndex;
    int32_t opId;
    std::set<std::string> matchType;
    int32_t reserveMaxCount;
    SortInitParam sortDesc;
    std::string opScope;
    std::unordered_set<std::string> forbidIndexs;
    ScanInfo scanInfo;
    bool useNest = false;
    std::vector<NestTableAttr> nestTableAttrs;
    std::string aggIndexName;
    std::vector<std::string> aggKeys;
    std::string aggType;
    std::string aggValueField;
    bool aggDistinct = false;
    std::map<std::string, std::pair<std::string, std::string>> aggRangeMap;
};

NAVI_TYPEDEF_PTR(ScanInitParamR);

} // namespace sql
