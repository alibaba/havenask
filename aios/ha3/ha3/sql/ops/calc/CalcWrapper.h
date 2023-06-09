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
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/framework/PushDownOp.h"
#include "table/Table.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace navi {
class GraphMemoryPoolResource;
} // namespace navi

namespace suez {
namespace turing {
class FunctionInterfaceCreator;
} // namespace turing
} // namespace suez

namespace isearch {
namespace sql {

class SqlBizResource;
class SqlQueryResource;
class MetaInfoResource;
class SqlSearchInfoCollector;

class CalcWrapper: public PushDownOp {
public:
    CalcWrapper(const CalcInitParam &param,
                SqlBizResource *bizResource,
                SqlQueryResource *queryResource,
                MetaInfoResource *metaInfoResource,
                navi::GraphMemoryPoolResource *memoryPoolResource);
    ~CalcWrapper();

public:
    bool init() override;
    bool compute(table::TablePtr &table, bool &isEof) override;
    void setReuseTable(bool reuse) override;
    bool isReuseTable() const override;
    std::string getName() const override {
        return "calc";
    }

public:
    void setMatchInfo(std::shared_ptr<suez::turing::MetaInfo> metaInfo,
                      const suez::turing::IndexInfoHelper *indexInfoHelper,
                      std::shared_ptr<indexlib::index::InvertedIndexReader> indexReader);

private:
    void incComputeTimes();
    void incOutputCount(int64_t count);
    void incTotalTime(int64_t time);
    void incTotalInputCount(size_t count);
    void reportMetrics();

private:
    CalcInitParam _param;
    CalcTablePtr _calcTablePtr;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    SqlSearchInfoCollector *_sqlSearchInfoCollector = nullptr;
    kmonitor::MetricsReporter *_opMetricReporter = nullptr;
    CalcInfo _calcInfo;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CalcWrapper> CalcWrapperPtr;
}  // namespace sql
}  // namespace isearch
