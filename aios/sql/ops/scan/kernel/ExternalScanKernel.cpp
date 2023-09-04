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
#include "sql/ops/scan/ExternalScanKernel.h"

#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/Resource.h"
#include "sql/data/TableType.h"
#include "sql/ops/externalTable/ha3sql/Ha3SqlRemoteScanR.h"

namespace sql {

ExternalScanKernel::ExternalScanKernel() {}

ExternalScanKernel::~ExternalScanKernel() {}

void ExternalScanKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.ExternalScanKernel")
        .output("output0", TableType::TYPE_ID)
        .dependOn(Ha3SqlRemoteScanR::RESOURCE_ID, true, BIND_RESOURCE_TO(_scanBase));
}

REGISTER_KERNEL(ExternalScanKernel);

} // namespace sql
