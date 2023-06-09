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

#include <memory>

#include "autil/HashAlgorithm.h"
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingIterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/in_mem_dynamic_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/normal/inverted_index/index_metrics.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index, DictionaryReader);
DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(index, DynamicSearchTree);
DECLARE_REFERENCE_CLASS(file_system, FileReader);
DECLARE_REFERENCE_CLASS(document, ModifiedTokens);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);

namespace indexlib { namespace index {
class SegmentPosting;
class BuildingIndexReader;
class IndexUpdateTermIterator;
}} // namespace indexlib::index

namespace indexlib { namespace index { namespace legacy {
class DynamicIndexReader
{
public:
    class TermUpdater
    {
    public:
        TermUpdater() = default;
        ~TermUpdater();
        TermUpdater(const TermUpdater&) = delete;
        TermUpdater(TermUpdater&& other);
        TermUpdater& operator=(TermUpdater&& other);
        TermUpdater(index::DictKeyInfo termKey, bool isNumberIndex,
                    DynamicIndexWriter::DynamicPostingTable* postingTable, DynamicIndexReader* reader);

        void Update(docid_t localDocId, bool isDelete);
        bool Empty() const { return _postingTable == nullptr; }

    private:
        index::DictKeyInfo _termKey;
        bool _isNumberIndex = false;
        DynamicIndexWriter::DynamicPostingTable* _postingTable = nullptr;
        DynamicIndexReader* _reader = nullptr;
    };

public:
    DynamicIndexReader();
    DynamicIndexReader(const DynamicIndexReader& other) = delete;
    DynamicIndexReader& operator=(const DynamicIndexReader& other) = delete;
    virtual ~DynamicIndexReader();

public:
    // TODO: return bool -> void
    bool Open(const config::IndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData,
              IndexMetrics* indexMetrics);
    void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics);

    // default poolSize 1000, sessionPool null
    index::Result<DynamicPostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize,
                                                  autil::mem_pool::Pool* sessionPool) const;

    index::Result<DynamicPostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                         uint32_t statePoolSize,
                                                         autil::mem_pool::Pool* sessionPool) const;

    void Update(docid_t docId, const document::ModifiedTokens& tokens);
    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete);

    TermUpdater GetTermUpdater(segmentid_t targetSegmentId, index::DictKeyInfo termKey);
    void UpdateBuildResourceMetrics();

public:
    void TEST_SetIndexConfig(const config::IndexConfigPtr& indexConfig)
    {
        _indexConfig = indexConfig;
        _indexFormatOption.Init(indexConfig);
    }

private:
    bool LoadSegments(const index_base::PartitionDataPtr& partitionData);
    DynamicPostingIterator* Lookup(const index::DictKeyInfo& key, const DocIdRangeVector& docIdRanges,
                                   uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool) const;
    void AddInMemSegment(docid_t baseDocId, const index_base::InMemorySegmentPtr& inMemSegment);
    void AddInMemSegmentReader(docid_t baseDocId, const InMemDynamicIndexSegmentReaderPtr& inMemSegReader);
    void FillSegmentContexts(const index::DictKeyInfo& key, autil::mem_pool::Pool* sessionPool,
                             std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const;
    void FillSegmentContextsByRanges(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                     autil::mem_pool::Pool* sessionPool,
                                     std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const;

private:
    IndexDictHasher _dictHasher;
    config::IndexConfigPtr _indexConfig;
    LegacyIndexFormatOption _indexFormatOption;
    std::vector<std::shared_ptr<file_system::ResourceFile>> _postingResources;
    std::vector<uint64_t> _segmentDocCount;
    std::vector<segmentid_t> _segmentIds;
    std::vector<docid_t> _baseDocIds;
    std::shared_ptr<BuildingIndexReader> _buildingIndexReader;
    IndexMetrics::SingleFieldIndexMetrics* _fieldIndexMetrics;
    index_base::TemperatureDocInfoPtr _temperatureDocInfo;

private:
    friend class DynamicIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DynamicIndexReader);

inline index::Result<DynamicPostingIterator*>
DynamicIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool) const
{
    index::DictKeyInfo key;
    if (!this->_dictHasher.GetHashKey(term, key)) {
        IE_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
               _indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, {}, statePoolSize, sessionPool);
}

inline index::Result<DynamicPostingIterator*>
DynamicIndexReader::PartialLookup(const index::Term& term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                  autil::mem_pool::Pool* sessionPool) const
{
    index::DictKeyInfo key;
    if (!this->_dictHasher.GetHashKey(term, key)) {
        IE_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
               _indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, ranges, statePoolSize, sessionPool);
}
}}} // namespace indexlib::index::legacy
