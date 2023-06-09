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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/ops/agg/AggBase.h"
#include "ha3/sql/ops/agg/AggFuncDesc.h"
#include "ha3/sql/ops/agg/Aggregator.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/N2OneKernel.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor
namespace navi {
class GraphMemoryPoolResource;
class KernelInitContext;
} // namespace navi

namespace isearch {
namespace sql {
class AggFuncManager;
class SqlBizResource;
class SqlQueryResource;
class SqlSearchInfoCollector;
class MetaInfoResource;
class CalcTable;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class AggKernel : public navi::N2OneKernel {
public:
    AggKernel();

private:
    AggKernel(const AggKernel &);
    AggKernel &operator=(const AggKernel &);

public:
    std::vector<navi::StaticResourceBindInfo> dependResources() const override;
    std::string inputPort() const override;
    std::string inputType() const override;
    std::string outputPort() const override;
    std::string outputType() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode collect(const navi::DataPtr &data) override;
    navi::ErrorCode finalize(navi::DataPtr &data) override;

private:
    void patchHintInfo(const std::map<std::string, std::map<std::string, std::string>> &hintsMap);
    void reportMetrics();
    void incComputeTime();
    void incOutputCount(uint32_t count);
    void incTotalTime(int64_t time);
    void incTotalInputCount(size_t count);

private:
    std::vector<std::string> _groupKeyVec;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
    std::vector<AggFuncDesc> _aggFuncDesc;
    AggHints _aggHints;
    std::string _scope;
    AggBasePtr _aggBase;
    kmonitor::MetricsReporter *_queryMetricsReporter = nullptr;
    AggInfo _aggInfo;
    SqlSearchInfoCollector *_sqlSearchInfoCollector = nullptr;
    AggFuncManager *_aggFuncManager = nullptr;
    int32_t _opId;
    std::unique_ptr<CalcTable> _calcTable;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;

    std::vector<int32_t> _reuseInputs;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggKernel> AggKernelPtr;
} // namespace sql
} // namespace isearch
