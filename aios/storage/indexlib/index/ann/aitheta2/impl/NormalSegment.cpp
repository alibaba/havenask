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
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"

#include "indexlib/index/ann/aitheta2/util/segment_data/ParallelMergeSegmentDataReader.h"

using namespace std;
using namespace indexlib::file_system;
using namespace aitheta2;
namespace indexlibv2::index::ann {

bool NormalSegment::Open()
{
    ANN_CHECK(_segmentMeta.Load(_directory), "load meta failed");
    ANN_CHECK(!_isOnline || IsServiceable(), "not serviceable. param[%s] not set in alter field merge",
              INDEX_BUILD_IN_FULL_BUILD_PHASE.c_str());

    return true;
}

bool NormalSegment::DumpSegmentMeta()
{
    _segmentMeta.SetSegmentSize(GetSegmentDataWriter()->GetSegmentSize());
    return _segmentMeta.Dump(_directory);
}

bool NormalSegment::IsServiceable() const
{
    for (const auto& [_, indexMeta] : _segmentMeta.GetIndexMetaMap()) {
        if (indexMeta.builderName == LINEAR_BUILDER && _indexConfig.buildConfig.builderName != LINEAR_BUILDER &&
            indexMeta.docCount > _indexConfig.buildConfig.buildThreshold) {
            return false;
        }
    }
    return true;
}

bool NormalSegment::GetDocCountInfo(const indexlib::file_system::DirectoryPtr& directory, DocCountMap& docCountMap)
{
    if (!SegmentMeta::IsExist(directory)) {
        return true;
    }

    SegmentMeta segmentMeta;
    ANN_CHECK(segmentMeta.Load(directory), "load segment meta failed");
    for (const auto& [indexId, indexMeta] : segmentMeta.GetIndexMetaMap()) {
        docCountMap[indexId] += indexMeta.docCount;
    }

    return true;
}

size_t NormalSegment::EstimateLoadMemoryUse(const indexlib::file_system::DirectoryPtr& directory)
{
    if (!SegmentMeta::IsExist(directory)) {
        return 0ul;
    }

    if (SegmentMeta::HasParallelMergeMeta(directory)) {
        return ParallelMergeSegmentDataReader::EstimateOpenMemoryUse(directory);
    }
    return SegmentDataReader::EstimateOpenMemoryUse(directory);
}

AUTIL_LOG_SETUP(indexlib.index, NormalSegment);
} // namespace indexlibv2::index::ann
