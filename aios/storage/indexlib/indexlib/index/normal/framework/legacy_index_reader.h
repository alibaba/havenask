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

#include "indexlib/config/index_config.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/framework/legacy_index_reader_interface.h"
#include "indexlib/index_base/index_meta/version.h"

namespace indexlib::index_base {
class PartitionData;
}

namespace indexlib::index {

class LegacyIndexReader : public InvertedIndexReader, public LegacyIndexReaderInterface
{
public:
    LegacyIndexReader();
    ~LegacyIndexReader();

public:
    // hintReader is used to reduce duplicated access to file_system and FronJsonString()
    // notice hintReader may be nullptr
    void Open(const std::shared_ptr<config::IndexConfig>& indexConfig,
              const std::shared_ptr<index_base::PartitionData>& partitionData,
              const InvertedIndexReader* hintReader = nullptr) override
    {
        assert(false);
    }
    size_t EstimateLoadSize(const std::shared_ptr<index_base::PartitionData>& partitionData,
                            const std::shared_ptr<config::IndexConfig>& indexConfig,
                            const index_base::Version& lastLoadVersion) override
    {
        assert(false);
        return 0;
    }
    void SetMultiFieldIndexReader(InvertedIndexReader* truncateReader) override {}
    void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics) override {}
    void SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessorReader) override {}
    std::shared_ptr<LegacyIndexAccessoryReader> GetLegacyAccessoryReader() const override { return nullptr; }

    void Update(docid_t docId, const document::ModifiedTokens& tokens) override { assert(false); }
    void Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete) override { assert(false); }
    // UpdateIndex is used to update patch format index.
    void UpdateIndex(IndexUpdateTermIterator* iter) override { assert(false); }

public:
    bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           indexlib::file_system::ReadOption, InvertedIndexSearchTracer* tracer) override
    {
        return GetSegmentPosting(key, segmentIdx, segPosting, tracer);
    }
    virtual bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                                   InvertedIndexSearchTracer* tracer)
    {
        assert(false);
        return false;
    }
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept override
    {
        assert(false);
        co_return false;
    }

    // to convert field names to fieldmap_t
    bool GenFieldMapMask(const std::string& indexName, const std::vector<std::string>& termFieldNames,
                         fieldmap_t& targetFieldMap) override;

protected:
    std::shared_ptr<indexlib::config::IndexConfig> GetIndexConfig() const;

private:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override
    {
        assert(false);
        return Status::Unimplement();
    }
    void SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessorReader) override {}

private:
    // using InvertedIndexReader::Open; // avoid 'hidden overloaded virtual' warning, -Werror,-Woverloaded-virtual
    IE_LOG_DECLARE();
};

inline std::shared_ptr<indexlib::config::IndexConfig> LegacyIndexReader::GetIndexConfig() const
{
    return std::dynamic_pointer_cast<indexlib::config::IndexConfig>(_indexConfig);
}

} // namespace indexlib::index
