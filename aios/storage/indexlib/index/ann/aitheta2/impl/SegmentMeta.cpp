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
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"

#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelMergeUtil.h"
#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelReduceMeta.h"

using namespace std;
using namespace autil::legacy;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

static constexpr const char* SEGMENT_META_FILE = "aitheta.segment.meta";
static constexpr const char* UNKNOWN_SEGMENT = "unknown";
static constexpr const char* NORMAL_SEGMENT = "normal";
static constexpr const char* REALTIME_SEGMENT = "realtime";

static constexpr const char* STATS_TYPE_JSON_STRING = "json_string";

TrainStats::TrainStats() : statsType(STATS_TYPE_JSON_STRING), stats {"{}"} {}

void TrainStats::Jsonize(JsonWrapper& json)
{
    json.Jsonize("stats_type", statsType, STATS_TYPE_JSON_STRING);
    json.Jsonize("stats", stats, "{}");
}

BuildStats::BuildStats() : statsType(STATS_TYPE_JSON_STRING), stats {"{}"} {}

void BuildStats::Jsonize(JsonWrapper& json)
{
    json.Jsonize("stats_type", statsType, STATS_TYPE_JSON_STRING);
    json.Jsonize("stats", stats, "{}");
}

void IndexMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("doc_count", docCount);
    json.Jsonize("builder_name", builderName);
    json.Jsonize("searcher_name", searcherName);
    json.Jsonize("train_stats", trainStats, TrainStats {});
    json.Jsonize("build_stats", buildStats, BuildStats {});
}

bool SegmentMeta::IsExist(const indexlib::file_system::DirectoryPtr& directory)
{
    return directory->IsExist(SEGMENT_META_FILE) || HasParallelMergeMeta(directory);
}

bool SegmentMeta::HasParallelMergeMeta(const indexlib::file_system::DirectoryPtr& directory)
{
    ParallelReduceMeta parallelReduceMeta;
    return parallelReduceMeta.Load(directory);
}

bool SegmentMeta::Load(const indexlib::file_system::DirectoryPtr& directory)
{
    if (directory->IsExist(SEGMENT_META_FILE)) {
        return DoLoad(directory);
    }

    ParallelReduceMeta parallelReduceMeta;
    ANN_CHECK(parallelReduceMeta.Load(directory), "load meta failed");
    vector<DirectoryPtr> directories;
    ParallelMergeUtil::GetParallelMergeDirs(directory, parallelReduceMeta, directories);

    for (size_t i = 0; i < directories.size(); ++i) {
        if (i == 0) {
            ANN_CHECK(DoLoad(directories[i]), "load meta failed");
        } else {
            SegmentMeta meta;
            ANN_CHECK(meta.DoLoad(directories[i]), "load meta failed");
            ANN_CHECK(Merge(meta), "merge meta failed");
        }
    }

    return true;
}

bool SegmentMeta::DoLoad(const indexlib::file_system::DirectoryPtr& directory)
{
    indexlib::file_system::ReaderOption readerOption(FSOT_MMAP);
    readerOption.mayNonExist = true;
    auto reader = directory->CreateFileReader(SEGMENT_META_FILE, readerOption);
    ANN_CHECK(reader, "create failed");
    string content((char*)reader->GetBaseAddress(), reader->GetLength());
    FromJsonString(*this, content);
    reader->Close().GetOrThrow();
    AUTIL_LOG(INFO, "segment meta load from[%s]", reader->DebugString().c_str());
    return true;
}

bool SegmentMeta::Merge(const SegmentMeta& segmentMeta)
{
    ANN_CHECK(_dimension == segmentMeta.GetDimension(), "dimension mismatch");
    SetSegmentSize(_segmentDataSize + segmentMeta.GetSegmentSize());
    for (auto& [indexId, indexMeta] : segmentMeta.GetIndexMetaMap()) {
        ANN_CHECK(AddIndexMeta(indexId, indexMeta), "merge index meta failed, index id[%lu] repeated in segment meta",
                  indexId);
    }
    return true;
}

bool SegmentMeta::Dump(const indexlib::file_system::DirectoryPtr& directory)
{
    directory->RemoveFile(SEGMENT_META_FILE, RemoveOption::MayNonExist());
    auto writer = directory->CreateFileWriter(SEGMENT_META_FILE);
    ANN_CHECK(writer, "create writer failed");
    string content = ToJsonString(*this);
    writer->Write(content.data(), content.size()).GetOrThrow();
    writer->Close().GetOrThrow();
    AUTIL_LOG(INFO, "segment meta dump to[%s]", writer->DebugString().c_str());
    return true;
}

void SegmentMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("segment_type", _segmentType);
    string type = UNKNOWN_SEGMENT;
    if (_segmentType == ST_NORMAL) {
        type = NORMAL_SEGMENT;
    } else if (_segmentType == ST_REALTIME) {
        type = REALTIME_SEGMENT;
    }
    json.Jsonize("segment_type_string", type);
    json.Jsonize("doc_count", _docCount);
    json.Jsonize("index_count", _indexCount);
    json.Jsonize("segment_data_size", _segmentDataSize);
    json.Jsonize("dimension", _dimension);
    json.Jsonize("index_info", _indexMetaMap);
    json.Jsonize("is_merged_segment", _isMergedSegment, _isMergedSegment);
}

AUTIL_LOG_SETUP(indexlib.index, SegmentMeta);

std::string SegmentType2Str(const SegmentType& type)
{
    static const std::vector<std::string> kType = {"Unknown", "NormalIndex", "RealtimeIndex"};
    return kType[static_cast<int16_t>(type)];
}

} // namespace indexlibv2::index::ann
