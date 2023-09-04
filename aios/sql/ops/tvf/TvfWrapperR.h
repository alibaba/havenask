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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "navi/common.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/common.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/tvf/InvocationAttr.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/TvfFuncFactoryR.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/Ha3QueryInfoR.h"
#include "sql/resource/ModelConfigMapR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "suez/turing/navi/SuezCavaAllocatorR.h"
#include "table/Table.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

struct TvfInitParam {
public:
    TvfInitParam();
    bool initFromJson(navi::KernelConfigContext &ctx);

public:
    std::string opName;
    std::string nodeName;
    InvocationAttr invocation;
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    std::vector<int32_t> reuseInputs;
    KernelScope scope;
    int32_t opId;
    int32_t reserveMaxCount;
};

class TvfWrapperR : public PushDownOp, public navi::Resource {
public:
    TvfWrapperR();
    ~TvfWrapperR();

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

public:
    bool init() override {
        return true;
    }
    bool compute(table::TablePtr &table, bool &isEof) override;
    std::string getName() const override {
        return "tvf";
    }
    void setReuseTable(bool reuse) override {
        _reuseTable = reuse;
    }
    bool isReuseTable() const override {
        return _reuseTable;
    }

public:
    bool compute(table::TablePtr &inputTable, bool &isEof, table::TablePtr &outputTable);
    const TvfInitParam &getInitParam() const {
        return _initParam;
    }

private:
    bool checkOutputTable(const table::TablePtr &table);
    void incComputeTimes();
    void incOutputCount(int64_t count);
    void incTotalTime(int64_t time);
    void incTotalInputCount(size_t count);
    void reportMetrics();

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    RESOURCE_DEPEND_ON(suez::turing::SuezCavaAllocatorR, _suezCavaAllocatorR);
    RESOURCE_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    RESOURCE_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    RESOURCE_DEPEND_ON(suez::turing::QueryMemPoolR, _queryMemPoolR);
    RESOURCE_DEPEND_ON(Ha3QueryInfoR, _ha3QueryInfoR);
    RESOURCE_DEPEND_ON(ModelConfigMapR, _modelConfigMapR);
    RESOURCE_DEPEND_ON(TvfFuncFactoryR, _tvfFuncFactoryR);
    TvfInitParam _initParam;
    TvfInfo _tvfInfo;
    TvfFuncPtr _tvfFunc;
    bool _reuseTable;
};

NAVI_TYPEDEF_PTR(TvfWrapperR);

} // namespace sql
