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
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentBuildResource.h"

namespace indexlibv2::index::ann {

struct RealtimeIndexBuildResource {
    index_id_t indexId = kInvalidIndexId;
    IndexMeta normalIndexMeta {};
    std::shared_ptr<IndexDataReader> normalIndexDataReader = nullptr;
};

class RealtimeSegmentBuildResource : public SegmentBuildResource
{
public:
    RealtimeSegmentBuildResource(const AithetaIndexConfig& indexConfig,
                                 const std::vector<std::shared_ptr<indexlib::file_system::Directory>>& indexerDirs);
    ~RealtimeSegmentBuildResource() = default;

public:
    std::shared_ptr<RealtimeIndexBuildResource> GetRealtimeIndexBuildResource(index_id_t indexId);
    std::vector<std::shared_ptr<RealtimeIndexBuildResource>> GetAllRealtimeIndexBuildResources();
    void Release();

private:
    AithetaIndexConfig _aithetaIndexConfig;
    std::vector<NormalSegmentPtr> _normalSegments;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann