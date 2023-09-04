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
#include "sql/resource/PartitionInfoR.h"

#include <engine/ResourceInitContext.h>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"

namespace sql {

const std::string PartitionInfoR::RESOURCE_ID = "partition_info_r";

PartitionInfoR::PartitionInfoR() {}

PartitionInfoR::~PartitionInfoR() {}

void PartitionInfoR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ_PART);
}

bool PartitionInfoR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode PartitionInfoR::init(navi::ResourceInitContext &ctx) {
    auto partCount = ctx.getPartCount();
    auto partId = ctx.getPartId();
    if (!autil::RangeUtil::getRange(partCount, partId, _range)) {
        NAVI_KERNEL_LOG(
            ERROR, "get part range failed, partCount: %d, partId: %d", partCount, partId);
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

const autil::PartitionRange &PartitionInfoR::getRange() const {
    return _range;
}

REGISTER_RESOURCE(PartitionInfoR);

} // namespace sql
