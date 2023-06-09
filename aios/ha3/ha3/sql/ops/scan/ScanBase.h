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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/ObjectTracer.h"
#include "autil/StringUtil.h"
#include "ha3/common/Query.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/sql/common/FieldInfo.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/ObjectPool.h"
#include "ha3/sql/common/TableDistribution.h"
#include "ha3/sql/common/TableMeta.h"
#include "ha3/sql/common/WatermarkType.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "ha3/sql/ops/sort/SortInitParam.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/turing/common/ModelConfig.h"
#include "indexlib/misc/common.h"
#include "iquan/common/catalog/LocationDef.h"
#include "navi/engine/KernelConfigContext.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace table {
class Table;
} // namespace table

namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
} // namespace matchdoc

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace isearch {
namespace sql {
class UdfToQueryManager;
} // namespace sql
} // namespace isearch
namespace suez {
namespace turing {
class CavaPluginManager;
class FunctionInterfaceCreator;
class SuezCavaAllocator;
class Tracer;
} // namespace turing
} // namespace suez

namespace isearch {
namespace search {
class MatchDataCollectorBase;
} // namespace search

namespace sql {

class PushDownOp;

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

struct ScanInitParam {
public:
    ScanInitParam();
    bool initFromJson(navi::KernelConfigContext &ctx);
    bool isRemoteScan();
    static void forbidIndex(
            const std::map<std::string, std::map<std::string, std::string>> &hintsMap,
            std::map<std::string, IndexInfo> &indexInfos,
            std::unordered_set<std::string> &forbidIndexs);

public:
    std::map<std::string, std::map<std::string, std::string>> hintsMap;
    std::map<std::string, IndexInfo> indexInfos;
    std::map<std::string, FieldInfo> fieldInfos;
    std::vector<std::string> outputFields;
    std::vector<std::string> rawOutputFields;
    std::vector<std::string> outputFieldsType;
    std::vector<std::string> hashFields;
    std::vector<NestTableAttr> nestTableAttrs;
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
    std::string conditionJson;
    std::string outputExprsJson;
    iquan::LocationDef location;
    TableDistribution tableDist;
    ScanResource scanResource;
    int64_t targetWatermark;
    WatermarkType targetWatermarkType;
    uint32_t batchSize;
    uint32_t limit;
    uint32_t parallelNum;
    uint32_t parallelIndex;
    int32_t opId;
    bool useNest;
    std::set<std::string> matchType;
    int32_t reserveMaxCount;
    SortInitParam sortDesc;
    std::string opScope;
    std::unordered_set<std::string> forbidIndexs;

    std::string aggIndexName;
    std::vector<std::string> aggKeys;
    std::string aggType;
    std::string aggValueField;
    bool aggDistinct;
    std::map<std::string, std::pair<std::string, std::string>> aggRangeMap;
private:
    AUTIL_LOG_DECLARE();
};

struct StreamQuery {
    common::QueryPtr query;
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
class ScanBase : public autil::ObjectTracer<ScanBase, true> {
public:
    ScanBase(const ScanInitParam &param);
    virtual ~ScanBase();

private:
    ScanBase(const ScanBase &);
    ScanBase &operator=(const ScanBase &);

public:
    bool init();
    virtual bool postInitPushDownOp(PushDownOp &pushDownOp) {
        return true;
    }
    bool batchScan(std::shared_ptr<table::Table> &table, bool &eof);
    bool updateScanQuery(const StreamQueryPtr &inputQuery);
    const search::MatchDataManagerPtr getMatchDataManager() {
        return _matchDataManager;
    }
    void setMatchDataCollectorBaseConfig(search::MatchDataCollectorBase *matchDataCollectorBase);
    bool getUseSub() const {
        return _useSub;
    }
    void setPushDownMode(bool pushDownMode) {
        _pushDownMode = pushDownMode;
    }

protected:
    virtual void
    patchHintInfo(const std::map<std::string, std::map<std::string, std::string>> &hintsMap);
    virtual void patchHintInfo(const std::map<std::string, std::string> &hintMap) {}
    std::shared_ptr<table::Table>
    createTable(std::vector<matchdoc::MatchDoc> &matchDocVec,
                const std::shared_ptr<matchdoc::MatchDocAllocator> &matchDocAllocator,
                bool reuseMatchDocAllocator);

    template <typename T>
    bool fromHint(const std::map<std::string, std::string> &hints, const std::string &key, T &val) {
        auto it = hints.find(key);
        if (it == hints.end()) {
            return false;
        }
        return autil::StringUtil::fromString(it->second, val);
    }
    bool prepareMatchDocAllocator();

private:
    bool preInit();
    virtual bool doInit() {
        return true;
    }
    virtual bool doBatchScan(std::shared_ptr<table::Table> &table, bool &eof) {
        return false;
    }
    virtual bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
        return false;
    }
    virtual std::shared_ptr<matchdoc::MatchDocAllocator> copyMatchDocAllocator(
            std::vector<matchdoc::MatchDoc> &matchDocVec,
            const std::shared_ptr<matchdoc::MatchDocAllocator> &matchDocAllocator,
            bool reuseMatchDocAllocator,
            std::vector<matchdoc::MatchDoc> &copyMatchDocs);
    virtual std::shared_ptr<table::Table>
    doCreateTable(std::shared_ptr<matchdoc::MatchDocAllocator> outputAllocator,
                  std::vector<matchdoc::MatchDoc> copyMatchDocs);

    void reportBaseMetrics();
    virtual void onBatchScanFinish() {}
    bool initExpressionCreator(
        const TableInfoPtr &tableInfo,
        bool useSub,
        suez::turing::FunctionInterfaceCreator *functionInterfaceCreator,
        suez::turing::CavaPluginManager *cavaPluginManager,
        suez::turing::SuezCavaAllocator *suezCavaAllocator,
        suez::turing::Tracer *tracer,
        indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot);
    void updateDurationTime(int64_t time);
    void incInitTime(int64_t time);
    void incComputeTime();

protected:
    void incUpdateScanQueryTime(int64_t time);
    void incSeekTime(int64_t time);
    void incEvaluateTime(int64_t time);
    void incOutputTime(int64_t time);
    void incTotalTime(int64_t time);
    void incTotalScanCount(int64_t count);
    void setTotalSeekCount(int64_t count);
    void incTotalOutputCount(int64_t count);
    void updateExtraInfo(std::string extraInfo);
    virtual std::string getTableNameForMetrics() {
        return _param.tableName;
    }

private:
    std::unique_ptr<autil::ScopedTime2> _durationTimer;

protected:
    bool _enableTableInfo;
    bool _useSub;
    bool _isLeader;
    // optional
    bool _scanOnce;
    bool _pushDownMode;
    uint32_t _batchSize;
    uint32_t _limit;
    uint32_t _seekCount; // statistics
    common::TimeoutTerminatorPtr _timeoutTerminator;
    // for lifetime
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    suez::turing::TableInfoPtr _tableInfo;
    std::shared_ptr<matchdoc::MatchDocAllocator> _matchDocAllocator;
    suez::turing::FunctionProviderPtr _functionProvider;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    ScanInfo _scanInfo;
    InnerScanInfo _innerScanInfo;
    const ScanInitParam &_param;
    std::string _tableMeta;
    isearch::turing::ModelConfigMap *_modelConfigMap = nullptr;
    UdfToQueryManager *_udfToQueryManager = nullptr;
    search::MatchDataManagerPtr _matchDataManager;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScanBase> ScanBasePtr;
} // namespace sql
} // namespace isearch
