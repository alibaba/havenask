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

#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"

namespace indexlibv2::index::ann {

enum SegmentType { ST_UNKNOWN = 0, ST_NORMAL, ST_REALTIME };
std::string SegmentType2Str(const SegmentType& type);

struct TrainStats : public autil::legacy::Jsonizable {
    std::string statsType;
    std::string stats;

    TrainStats();

    void Jsonize(JsonWrapper& json) override;
};

struct BuildStats : public autil::legacy::Jsonizable {
    std::string statsType;
    std::string stats;

    BuildStats();

    void Jsonize(JsonWrapper& json) override;
};

struct IndexMeta : public autil::legacy::Jsonizable {
    size_t docCount {0u};
    std::string builderName;
    std::string searcherName;
    TrainStats trainStats;
    BuildStats buildStats;

    void Jsonize(JsonWrapper& json) override;
};

typedef std::map<index_id_t, IndexMeta> IndexMetaMap;

class SegmentMeta : public autil::legacy::Jsonizable
{
public:
    static bool IsExist(const indexlib::file_system::DirectoryPtr&);
    static bool HasParallelMergeMeta(const indexlib::file_system::DirectoryPtr&);
    bool Load(const indexlib::file_system::DirectoryPtr&);
    bool Dump(const indexlib::file_system::DirectoryPtr&);

    SegmentType GetSegmentType() const { return _segmentType; }
    void SetSegmentType(SegmentType type) { _segmentType = type; }
    uint32_t GetDimension() const { return _dimension; }
    void SetDimension(uint32_t dimension) { _dimension = dimension; }
    bool AddIndexMeta(index_id_t id, const IndexMeta& meta);
    bool GetIndexMeta(index_id_t id, IndexMeta& meta) const;
    void SetSegmentSize(size_t size) { _segmentDataSize = size; }
    size_t GetSegmentSize() const { return _segmentDataSize; }
    size_t GetDocCount() const { return _docCount; }
    size_t GetIndexCount() const { return _indexCount; }
    const IndexMetaMap& GetIndexMetaMap() const { return _indexMetaMap; }
    bool IsMergedSegment() const { return _isMergedSegment; }
    void SetMergedSegment(bool isMergedSegment) { _isMergedSegment = isMergedSegment; }

private:
    void Jsonize(JsonWrapper& json) override;
    bool DoLoad(const indexlib::file_system::DirectoryPtr&);
    bool Merge(const SegmentMeta& meta);

private:
    SegmentType _segmentType {ST_UNKNOWN};
    uint32_t _dimension {0u};
    size_t _docCount {0ul};
    size_t _indexCount {0ul};
    size_t _segmentDataSize {0ul};
    IndexMetaMap _indexMetaMap {};
    bool _isMergedSegment = false;
    AUTIL_LOG_DECLARE();
};

inline bool SegmentMeta::GetIndexMeta(index_id_t id, IndexMeta& meta) const
{
    auto iterator = _indexMetaMap.find(id);
    if (iterator == _indexMetaMap.end()) {
        return false;
    }
    meta = iterator->second;
    return true;
}

inline bool SegmentMeta::AddIndexMeta(index_id_t id, const IndexMeta& meta)
{
    bool ret = _indexMetaMap.emplace(id, meta).second;
    if (ret) {
        ++_indexCount;
        _docCount += meta.docCount;
    }
    return ret;
}

} // namespace indexlibv2::index::ann
