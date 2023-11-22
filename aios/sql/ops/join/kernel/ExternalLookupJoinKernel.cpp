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
#include "sql/ops/join/ExternalLookupJoinKernel.h"

#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Resource.h"
#include "sql/data/TableType.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/externalTable/ha3sql/Ha3SqlRemoteScanR.h"
#include "sql/ops/join/JoinKernelBase.h"
#include "sql/ops/join/LookupR.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/resource/WatermarkR.h"

namespace sql {

ExternalLookupJoinKernel::ExternalLookupJoinKernel() {}

ExternalLookupJoinKernel::~ExternalLookupJoinKernel() {}

void ExternalLookupJoinKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.ExternalLookupJoinKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .dependOn(Ha3SqlRemoteScanR::RESOURCE_ID, true, BIND_RESOURCE_TO(_scanBase))
        .dependOn(LookupR::RESOURCE_ID, true, BIND_RESOURCE_TO(_lookupR))
        .resourceConfigKey(ScanInitParamR::RESOURCE_ID, "build_node")
        .resourceConfigKey(CalcInitParamR::RESOURCE_ID, "build_node")
        .resourceConfigKey(WatermarkR::RESOURCE_ID, "build_node")
        .jsonAttrs(R"json(
{
    "remote_scan_init_fire" : false
})json");
}

REGISTER_KERNEL(ExternalLookupJoinKernel);

} // namespace sql
