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
#include "indexlib/base/Status.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/primary_key/Constant.h"
#include "indexlib/index/primary_key/PrimaryKeyDiskIndexer.h"
#include "indexlib/index/primary_key/SegmentDataAdapter.h"

namespace indexlibv2::index {

class PrimaryKeyLoadPlan : public autil::NoCopyable
{
public:
    PrimaryKeyLoadPlan() {}
    ~PrimaryKeyLoadPlan() {}

public:
    void Init(docid64_t baseDocid) { _baseDocid = baseDocid; }
    inline void AddSegmentData(const SegmentDataAdapter::SegmentDataType& segmentData);
    inline docid64_t GetBaseDocId() const { return _baseDocid; }
    inline size_t GetDocCount() const { return _docCount; }
    inline size_t GetLastSegmentDocCount() const
    {
        return _segments.size() > 0 ? _segments[_segments.size() - 1]._segmentInfo->docCount : 0;
    }

    inline std::string GetTargetSliceFileName() const;
    inline std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetPrimaryKeyDirectory(const std::shared_ptr<config::InvertedIndexConfig>& indexConfig);
    void GetSegmentIdList(std::vector<segmentid_t>* segmentIds) const
    {
        for (auto& segmentData : _segments) {
            segmentIds->push_back(segmentData._segmentId);
        }
    }
    size_t GetSegmentNum() const { return _segments.size(); }
    std::vector<SegmentDataAdapter::SegmentDataType>& GetLoadSegmentDatas() { return _segments; }

    template <typename Key>
    bool SetSegmentIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           std::shared_ptr<PrimaryKeyDiskIndexer<Key>> indexer)
    {
        assert(_segments.size() > 0);
        std::string indexType = indexConfig->GetIndexType();
        std::string indexName = indexConfig->GetIndexName();
        auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create factory failed for type [%s]", indexType.c_str());
            return false;
        }

        // It's very complicated to load pk index due to combine-pk.
        // When segment opened, pk is created but not opened.
        // When reader opened, one pk indexer may open many segments
        // example:  pk open segments [0 1 2 3 4]
        // We fill pk indexer into last segment (4), and fill empty pk into 0-3 for pk attribute loading
        // If a segment already has pk indexer, we should remove this (clean old reader's combined-pk indexer)
        for (size_t i = 0; i < _segments.size(); i++) {
            auto segment = _segments[i]._segment;
            if (!segment) {
                continue;
            }
            // for combine mode delete before segment disk indexer
            decltype(indexer) newIndexer = indexer;
            if (i != _segments.size() - 1) {
                // add for pk attribute need a plain disk indexer
                DiskIndexerParameter indexerParam;
                indexerParam.docCount = segment->GetSegmentInfo()->GetDocCount();
                newIndexer = std::dynamic_pointer_cast<PrimaryKeyDiskIndexer<Key>>(
                    indexFactory->CreateDiskIndexer(indexConfig, indexerParam));
            }
            if (!newIndexer) {
                AUTIL_LOG(ERROR, "not indexer to add");
                return false;
            }
            auto [st, oldIndexer] = segment->GetIndexer(indexType, indexName);
            if (st.IsOK()) {
                std::shared_ptr<PrimaryKeyDiskIndexer<Key>> oldTypedIndexer;
                if (oldIndexer) {
                    oldTypedIndexer = std::dynamic_pointer_cast<PrimaryKeyDiskIndexer<Key>>(oldIndexer);
                    if (oldTypedIndexer) {
                        indexer->InhertPkAttributeDiskIndexer(oldTypedIndexer.get());
                    }
                }
            }
            segment->DeleteIndexer(indexType, indexName);
            segment->AddIndexer(indexType, indexName, newIndexer);
        }
        return true;
    }

private:
    std::vector<SegmentDataAdapter::SegmentDataType> _segments;
    docid64_t _baseDocid = INVALID_DOCID;
    size_t _docCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

inline void PrimaryKeyLoadPlan::AddSegmentData(const SegmentDataAdapter::SegmentDataType& segmentData)
{
    _segments.push_back(segmentData);
    _docCount += segmentData._segmentInfo->docCount;
}

inline std::string PrimaryKeyLoadPlan::GetTargetSliceFileName() const
{
    std::stringstream ss;
    ss << PRIMARY_KEY_DATA_SLICE_FILE_NAME;
    for (size_t i = 0; i < _segments.size(); i++) {
        ss << "_" << _segments[i]._segmentId;
    }
    return ss.str();
}

inline std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
PrimaryKeyLoadPlan::GetPrimaryKeyDirectory(const std::shared_ptr<config::InvertedIndexConfig>& indexConfig)
{
    assert(_segments.size() > 0);
    SegmentDataAdapter::SegmentDataType maxSegData = _segments[_segments.size() - 1];
    auto segmentDirectory = maxSegData._directory;
    assert(segmentDirectory);
    auto segIDir = segmentDirectory->GetIDirectory();
    assert(segIDir);
    return segIDir->GetDirectory(INDEX_DIR_NAME).StatusWith();
}

} // namespace indexlibv2::index
