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

#include <map>
#include <memory>
#include <vector>

#include "indexlib/index/common/patch/PatchWriter.h"
#include "indexlib/util/ExpandableBitmap.h"

namespace indexlibv2::index {

class DeletionMapPatchWriter : public PatchWriter
{
public:
    DeletionMapPatchWriter(std::shared_ptr<indexlib::file_system::IDirectory> workDir,
                           const std::map<segmentid_t, size_t>& segmentId2DocCount);
    ~DeletionMapPatchWriter() = default;

public:
    Status Write(segmentid_t targetSegmentId, docid_t localDocId);

public:
    Status Close() override;
    Status GetPatchFileInfos(PatchFileInfos* patchFileInfos) override;

public:
    static int64_t EstimateMemoryUse(const std::map<segmentid_t, size_t>& segmentId2DocCount);
    const std::map<segmentid_t, std::shared_ptr<indexlib::util::ExpandableBitmap>>& GetSegmentId2Bitmaps() const;

private:
    std::map<segmentid_t, std::shared_ptr<indexlib::util::ExpandableBitmap>> _segmentId2Bitmaps;
    std::map<segmentid_t, size_t> _segmentId2DocCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
