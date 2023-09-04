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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "table/Table.h"

namespace indexlib {
namespace index {
class InvertedIndexReader;
} // namespace index
} // namespace indexlib
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi
namespace suez {
namespace turing {
class IndexInfoHelper;
class MetaInfo;
} // namespace turing
} // namespace suez

namespace sql {

class CalcWrapperR : public PushDownOp, public navi::Resource {
public:
    CalcWrapperR();
    ~CalcWrapperR();

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static constexpr char RESOURCE_ID[] = "calc_wrapper_r";

public:
    bool init() override {
        return true;
    }
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
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(CalcInitParamR, _calcInitParamR);
    RESOURCE_DEPEND_ON(CalcTableR, _calcTableR);
    RESOURCE_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    CalcInfo _calcInfo;
    int32_t _opId = -1;
};

NAVI_TYPEDEF_PTR(CalcWrapperR);

} // namespace sql
