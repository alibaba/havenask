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
#include "ha3/sql/resource/MetaInfoResource.h"

#include "navi/common.h"

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, MetaInfoResource);

MetaInfoResource::MetaInfoResource() {}

MetaInfoResource::~MetaInfoResource() {}

void MetaInfoResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(navi::META_INFO_RESOURCE_ID);
}

bool MetaInfoResource::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode MetaInfoResource::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

void MetaInfoResource::preSerialize() {
    NAVI_KERNEL_LOG(TRACE3, "start pre-serialize");
    _overwriteInfoCollector.shrinkOverwrite();
    _mergeInfoCollector.mergeSqlSearchInfo(_overwriteInfoCollector.stealSqlSearchInfo());
}

void MetaInfoResource::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(META_INFO_RESOURCE_CHECKSUM);
    _mergeInfoCollector.serialize(dataBuffer);
    NAVI_KERNEL_LOG(TRACE3, "end serialize to data buffer, length [%d]", dataBuffer.getDataLen());
}

bool MetaInfoResource::merge(autil::DataBuffer &dataBuffer) {
    NAVI_KERNEL_LOG(TRACE3, "start merge from data buffer, length [%d]", dataBuffer.getDataLen());
    size_t checksum = 0;
    dataBuffer.read(checksum);
    if (checksum != META_INFO_RESOURCE_CHECKSUM) {
        NAVI_KERNEL_LOG(ERROR,
                        "invalid checksum, expect [0x%lx] actual [0x%lx]",
                        META_INFO_RESOURCE_CHECKSUM,
                        checksum);
        return false;
    }

    SqlSearchInfoCollector searchInfoCollector;
    if (!searchInfoCollector.deserialize(dataBuffer)) {
        NAVI_KERNEL_LOG(ERROR, "deserialize search info collector failed");
        return false;
    }
    _mergeInfoCollector.mergeSqlSearchInfo(searchInfoCollector.stealSqlSearchInfo());

    return true;
}

void MetaInfoResource::finalize() {
    NAVI_KERNEL_LOG(TRACE3, "start finalize");
    _overwriteInfoCollector.shrinkOverwrite();
    _mergeInfoCollector.mergeSqlSearchInfo(_overwriteInfoCollector.stealSqlSearchInfo());    
}

REGISTER_RESOURCE(MetaInfoResource);

} // namespace sql
} // namespace isearch
