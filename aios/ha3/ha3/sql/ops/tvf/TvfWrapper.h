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

#include "autil/Log.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/InvocationAttr.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/framework/PushDownOp.h"
#include "table/Table.h"
#include "navi/engine/KernelConfigContext.h"

namespace navi {
class GraphMemoryPoolResource;
}  // namespace navi
namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace isearch {
namespace sql {

class SqlBizResource;
class SqlQueryResource;
class MetaInfoResource;
class SqlSearchInfoCollector;

struct TvfInitParam {
public:
    TvfInitParam();
    bool initFromJson(navi::KernelConfigContext &ctx);

public:
    InvocationAttr invocation;
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    std::vector<int32_t> reuseInputs;
    KernelScope scope;
    int32_t opId;
    int32_t reserveMaxCount;
    std::string opName;
    std::string nodeName;
};

class TvfWrapper : public PushDownOp {
public:
    TvfWrapper(const TvfInitParam &initParam,
               SqlBizResource *bizResource,
               SqlQueryResource *queryResource,
               MetaInfoResource *metaInfoResource,
               navi::GraphMemoryPoolResource *memoryPoolResource);
    ~TvfWrapper();

public:
    bool init() override;
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

private:
    bool checkOutputTable(const table::TablePtr &table);
    void incComputeTimes();
    void incOutputCount(int64_t count);
    void incTotalTime(int64_t time);
    void incTotalInputCount(size_t count);
    void reportMetrics();

private:
    TvfInitParam _initParam;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    kmonitor::MetricsReporter *_queryMetricsReporter = nullptr;
    SqlSearchInfoCollector *_sqlSearchInfoCollector = nullptr;
    TvfInfo _tvfInfo;
    TvfFuncPtr _tvfFunc;
    bool _reuseTable;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TvfWrapper> TvfWrapperPtr;

}  // namespace isearch
}  // namespace sql
