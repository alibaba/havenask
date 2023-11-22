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

#include <algorithm>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/ops/join/HashJoinMapR.h"
#include "sql/ops/join/JoinBaseParamR.h"
#include "sql/ops/join/JoinInfoCollectorR.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace sql {

class JoinKernelBase : public navi::Kernel {
public:
    static const int DEFAULT_BATCH_SIZE;

public:
    JoinKernelBase();
    virtual ~JoinKernelBase();

public:
    void def(navi::KernelDefBuilder &builder) const override {}
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    virtual bool doInit() {
        return true;
    }
    virtual bool doConfig(navi::KernelConfigContext &ctx) {
        return true;
    }

protected:
    bool
    createHashMap(const table::TablePtr &table, size_t offset, size_t count, bool hashLeftTable);
    // lookup join or left multi join
    bool getLookupInput(navi::KernelComputeContext &runContext,
                        const navi::PortIndex &portIndex,
                        bool leftTable,
                        table::TablePtr &inputTable,
                        bool &eof);
    // hash join or nested loop join
    bool getInput(navi::KernelComputeContext &runContext,
                  const navi::PortIndex &portIndex,
                  bool leftTable,
                  size_t bufferLimitSize,
                  table::TablePtr &inputTable,
                  bool &eof);

    static bool canTruncate(size_t joinedCount, size_t truncateThreshold);

protected:
    virtual void reportMetrics();
    void incTotalLeftInputTable(size_t count);
    void incTotalRightInputTable(size_t count);

private:
    void patchHintInfo(const std::map<std::string, std::string> &hintsMap);

private:
    KERNEL_DEPEND_DECLARE();

protected:
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    KERNEL_DEPEND_ON(JoinInfoCollectorR, _joinInfoR);
    KERNEL_DEPEND_ON(JoinBaseParamR, _joinParamR);
    KERNEL_DEPEND_ON(HashJoinMapR, _hashJoinMapR);
    std::vector<int32_t> _reuseInputs;
    size_t _batchSize;
    size_t _truncateThreshold;
    int32_t _opId;
};

typedef std::shared_ptr<JoinKernelBase> JoinKernelBasePtr;

} // namespace sql
