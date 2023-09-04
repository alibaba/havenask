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
#include "indexlib/index/attribute/SegmentUpdateBitmap.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/patch/PatchMerger.h"

namespace indexlibv2::index {

class AttributePatchMerger : public PatchMerger
{
public:
    AttributePatchMerger(const std::shared_ptr<AttributeConfig>& attrConfig,
                         const std::shared_ptr<SegmentUpdateBitmap>& segUpdateBitmap)
        : _attrConfig(attrConfig)
        , _segUpdateBitmap(segUpdateBitmap)
    {
    }
    virtual ~AttributePatchMerger() = default;

    std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreateTargetPatchFileWriter(std::shared_ptr<indexlib::file_system::IDirectory> targetSegmentDir,
                                segmentid_t srcSegmentId, segmentid_t targetSegmentId) override;

protected:
    std::shared_ptr<AttributeConfig> _attrConfig;
    std::shared_ptr<SegmentUpdateBitmap> _segUpdateBitmap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
