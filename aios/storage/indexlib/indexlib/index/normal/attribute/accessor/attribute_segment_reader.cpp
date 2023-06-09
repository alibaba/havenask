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
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"

#include "autil/EnvUtil.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/util/segment_file_compress_util.h"
#include "indexlib/index_define.h"
#include "indexlib/util/MemBuffer.h"

using namespace indexlib::util;

namespace indexlib { namespace index {

bool AttributeSegmentReader::SupportFileCompress(const config::AttributeConfigPtr& attrConfig,
                                                 const index_base::SegmentInfo& segInfo) const
{
    return SegmentFileCompressUtil::SupportFileCompress(attrConfig->GetFileCompressConfig(), segInfo);
}

void AttributeSegmentReader::EnableGlobalReadContext()
{
    if (mGlobalCtx) {
        return;
    }

    mGlobalCtxSwitchLimit = autil::EnvUtil::getEnv("GLOBAL_READ_CONTEXT_SWITCH_MEMORY_LIMIT", 2 * 1024 * 1024);
    mGlobalCtxPool.reset(new autil::mem_pool::UnsafePool());
    mGlobalCtx = CreateReadContextPtr(mGlobalCtxPool.get());
}

}} // namespace indexlib::index
