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
#include <string>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GigClientR.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/TableDistribution.h"
#include "sql/ops/tableSplit/TableSplit.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/HashFunctionCacheR.h"
#include "table/Row.h"
#include "table/Table.h"

namespace navi {
class GraphDef;
class GroupDatas;
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
struct OverrideData;
} // namespace navi

namespace sql {

class DelayDpKernel : public navi::Kernel {
public:
    struct InputInfo {
        table::TablePtr table;
        std::vector<std::vector<table::Row>> partRows;
        TableSplit splitUtil;
    };

public:
    DelayDpKernel();
    ~DelayDpKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    bool initStringAttr(navi::KernelConfigContext &ctx, const std::string &key, std::string &value);
    bool initVectorAttr(navi::KernelConfigContext &ctx,
                        const std::string &key,
                        std::vector<std::string> &values);
    bool initGraphDef(navi::KernelConfigContext &ctx);
    bool mergeInputData(navi::GroupDatas &datas);
    bool splitInputData();
    bool redirectGraphInput(std::vector<navi::OverrideData> &overrideDatas);
    bool partHasData(int partId);
    void addOverrideDatas(int partId, std::vector<navi::OverrideData> &overrideDatas);
    navi::ErrorCode forkGraph(navi::KernelComputeContext &runContext);

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    KERNEL_DEPEND_ON_FALSE(navi::GigClientR, _gigClientR);
    KERNEL_DEPEND_ON_FALSE(HashFunctionCacheR, _hashFunctionCacheR);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    std::vector<std::string> _inputNames;
    std::vector<TableDistribution> _inputDists;
    std::vector<std::string> _outputNames;
    std::unique_ptr<navi::GraphDef> _graphDef;
    bool _debug {false};
    int _partCnt {1};

    std::vector<InputInfo> _inputInfos;
    bool _broadcast {true};
    std::vector<int> _validPartIds;
};

} // namespace sql
