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
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/patch/PatchWriter.h"
#include "indexlib/index/inverted_index/patch/IInvertedIndexSegmentUpdater.h"

namespace indexlibv2::config {
class IIndexConfig;
class InvertedIndexConfig;
} // namespace indexlibv2::config

namespace indexlib::index {
class InvertedIndexPatchWriter : public indexlibv2::index::PatchWriter
{
public:
    InvertedIndexPatchWriter(std::shared_ptr<file_system::IDirectory> workDir, segmentid_t srcSegmentId,
                             const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);
    ~InvertedIndexPatchWriter() = default;

public:
    Status Write(segmentid_t targetSegmentId, docid_t localDocId, DictKeyInfo termKey, bool isDelete);

public:
    Status Close() override;
    virtual Status GetPatchFileInfos(indexlibv2::index::PatchFileInfos* patchFileInfos) override;

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::map<segmentid_t, std::unique_ptr<IInvertedIndexSegmentUpdater>> _segmentId2Updaters;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
