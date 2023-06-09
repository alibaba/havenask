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
#include "build_service/custom_merger/MergeResourceProvider.h"

#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"

using namespace std;
using namespace indexlib::partition;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, MergeResourceProvider);

MergeResourceProvider::MergeResourceProvider() : _checkpointVersionId(-1) {}

MergeResourceProvider::~MergeResourceProvider() {}

bool MergeResourceProvider::init(const std::string& indexPath, const IndexPartitionOptions& options,
                                 const IndexPartitionSchemaPtr& newSchema, const std::string& pluginPath,
                                 int32_t targetVersionId, int32_t checkpointVersionId)
{
    assert(newSchema);
    _newSchema = newSchema;
    _indexProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(indexPath, options, pluginPath,
                                                                                     targetVersionId);
    _checkpointVersionId = checkpointVersionId;
    return _indexProvider.get() != nullptr;
}

void MergeResourceProvider::getIndexSegmentInfos(std::vector<SegmentInfo>& segmentInfos)
{
    auto ite = _indexProvider->CreatePartitionSegmentIterator();
    Version checkpointVersion;
    if (_checkpointVersionId != -1) {
        VersionLoader::GetVersionS(_indexProvider->GetIndexPath(), checkpointVersion, _checkpointVersionId);
    }

    while (ite->IsValid()) {
        if (checkpointVersion.HasSegment(ite->GetSegmentId())) {
            ite->MoveToNext();
            continue;
        }
        SegmentInfo info;
        info.segmentId = ite->GetSegmentId();
        info.docCount = ite->GetSegmentData().GetSegmentInfo()->docCount;
        segmentInfos.push_back(info);
        ite->MoveToNext();
    }
}

}} // namespace build_service::custom_merger
