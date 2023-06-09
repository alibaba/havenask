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
#include "indexlib/index/common/patch/PatchMerger.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}
namespace indexlib::index {
class SingleFieldIndexSegmentPatchIterator;
}

namespace indexlib::index {
class InvertedIndexPatchMerger : public indexlibv2::index::PatchMerger
{
public:
    InvertedIndexPatchMerger(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedIndexConfig);
    ~InvertedIndexPatchMerger() {};

public:
    std::pair<Status, std::shared_ptr<file_system::FileWriter>>
    CreateTargetPatchFileWriter(std::shared_ptr<file_system::IDirectory> targetSegmentDir, segmentid_t srcSegmentId,
                                segmentid_t targetSegmentId) override;

    Status Merge(const indexlibv2::index::PatchFileInfos& patchFileInfos,
                 const std::shared_ptr<file_system::FileWriter>& targetPatchFileWriter) override;

private:
    Status DoMerge(std::unique_ptr<SingleFieldIndexSegmentPatchIterator> patchIter,
                   const std::shared_ptr<file_system::FileWriter>& targetPatchFileWriter);

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _invertedIndexConfig;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
