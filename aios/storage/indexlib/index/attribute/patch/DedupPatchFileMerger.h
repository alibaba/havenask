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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/attribute/AttributeUpdateBitmap.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/patch/PatchFileMerger.h"

namespace indexlibv2::index {

class DedupPatchFileMerger : public PatchFileMerger
{
public:
    DedupPatchFileMerger(const std::shared_ptr<config::AttributeConfig>& attrConfig, const PatchInfos& allPatchInfos,
                         const std::shared_ptr<AttributeUpdateBitmap>& attrUpdateBitmap)
        : PatchFileMerger(allPatchInfos)
        , _attrConfig(attrConfig)
        , _attrUpdateBitmap(attrUpdateBitmap)
    {
    }
    ~DedupPatchFileMerger() = default;

public:
    std::shared_ptr<PatchMerger> CreatePatchMerger(segmentid_t segId) const override;
    Status FindPatchFiles(const IIndexMerger::SegmentMergeInfos& segMergeInfos, PatchInfos* patchInfos) override;

private:
    std::shared_ptr<config::AttributeConfig> _attrConfig;
    std::shared_ptr<AttributeUpdateBitmap> _attrUpdateBitmap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
