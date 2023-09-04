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
#include "sql/ops/metaCollect/MetaMergeKernel.h"

#include <memory>
#include <utility>

#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "sql/ops/metaCollect/SqlMetaData.h"
#include "sql/proto/SqlSearchInfo.pb.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace sql {

const std::string MetaMergeKernel::KERNEL_ID = "sql.meta_merge";
const std::string MetaMergeKernel::INPUT_PORT = "in";
const std::string MetaMergeKernel::OUTPUT_PORT = "out";

MetaMergeKernel::MetaMergeKernel() {}

MetaMergeKernel::~MetaMergeKernel() {}

void MetaMergeKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name(KERNEL_ID)
        .inputGroup(INPUT_PORT, SqlMetaDataType::TYPE_ID)
        .output(OUTPUT_PORT, SqlMetaDataType::TYPE_ID);
}

bool MetaMergeKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode MetaMergeKernel::init(navi::KernelInitContext &ctx) {
    _collector = std::make_shared<SqlSearchInfoCollector>();
    return navi::EC_NONE;
}

navi::ErrorCode MetaMergeKernel::compute(navi::KernelComputeContext &ctx) {
    navi::GroupDatas datas;
    ctx.getGroupInput(0, datas);
    for (const auto &streamData : datas) {
        auto metaData = dynamic_cast<SqlMetaData *>(streamData.data.get());
        if (!metaData) {
            continue;
        }
        const auto &collector = metaData->getCollector();
        if (!collector) {
            continue;
        }
        SqlSearchInfo tmp;
        collector->swap(tmp);
        _collector->mergeSqlSearchInfo(std::move(tmp));
    }
    if (datas.eof()) {
        _collector->shrinkOverwrite();
        auto metaData = std::make_shared<SqlMetaData>(std::move(_collector));
        ctx.setOutput(0, metaData, true);
    }
    return navi::EC_NONE;
}

REGISTER_KERNEL(MetaMergeKernel);

} // namespace sql
