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
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentBuildResource.h"

using namespace std;
namespace indexlibv2::index::ann {

AUTIL_LOG_SETUP(indexlib.index, RealtimeSegmentBuildResource);

RealtimeSegmentBuildResource::RealtimeSegmentBuildResource(
    const AithetaIndexConfig& indexConfig,
    const std::vector<std::shared_ptr<indexlib::file_system::Directory>>& indexerDirs)
    : _aithetaIndexConfig(indexConfig)
{
    for (const auto& indexerDir : indexerDirs) {
        auto normalSegment = make_shared<NormalSegment>(indexerDir, indexConfig, true);
        if (normalSegment->Open()) {
            _normalSegments.push_back(normalSegment);
        }
    }
}

std::vector<std::shared_ptr<RealtimeIndexBuildResource>>
RealtimeSegmentBuildResource::GetAllRealtimeIndexBuildResources()
{
    std::set<index_id_t> indexIds;
    for (const auto& segment : _normalSegments) {
        for (auto& [indexId, _] : segment->GetSegmentMeta().GetIndexMetaMap()) {
            indexIds.insert(indexId);
        }
    }

    std::vector<std::shared_ptr<RealtimeIndexBuildResource>> resources;
    for (index_id_t indexId : indexIds) {
        auto resource = GetRealtimeIndexBuildResource(indexId);
        if (resource != nullptr) {
            resources.push_back(resource);
        }
    }

    return resources;
}

std::shared_ptr<RealtimeIndexBuildResource>
RealtimeSegmentBuildResource::GetRealtimeIndexBuildResource(index_id_t indexId)
{
    IndexMeta retIndexMeta;
    IndexDataReaderPtr retIndexDataReader;
    for (const auto& segment : _normalSegments) {
        IndexMeta indexMeta;
        if (!segment->GetSegmentMeta().GetIndexMeta(indexId, indexMeta)) {
            continue;
        }
        auto segDataReader = segment->GetSegmentDataReader();
        if (segDataReader == nullptr) {
            AUTIL_LOG(INFO, "segment does not exist, try next");
            continue;
        }
        auto indexDataReader = segDataReader->GetIndexDataReader(indexId);
        if (indexDataReader == nullptr) {
            continue;
        }
        if (retIndexDataReader == nullptr || retIndexMeta.docCount < indexMeta.docCount) {
            retIndexMeta = indexMeta;
            retIndexDataReader = indexDataReader;
        }
    }

    if (retIndexDataReader == nullptr) {
        return nullptr;
    }

    auto resource = std::make_shared<RealtimeIndexBuildResource>();
    resource->normalIndexMeta = retIndexMeta;
    resource->normalIndexDataReader = retIndexDataReader;
    resource->indexId = indexId;
    return resource;
}

void RealtimeSegmentBuildResource::Release()
{
    for (auto segment : _normalSegments) {
        segment->Close();
    }
    AUTIL_LOG(INFO, "release realtime segment build resource");
}

} // namespace indexlibv2::index::ann