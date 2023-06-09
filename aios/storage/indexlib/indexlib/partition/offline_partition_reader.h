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
#ifndef __INDEXLIB_OFFLINE_PARTITION_READER_H
#define __INDEXLIB_OFFLINE_PARTITION_READER_H

#include <map>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition_reader.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, KKVReader);
DECLARE_REFERENCE_CLASS(index, KVReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(index, SourceReader);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

namespace indexlib { namespace partition {

class OfflinePartitionReader : public OnlinePartitionReader
{
public:
    OfflinePartitionReader(
        const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
        const util::SearchCachePartitionWrapperPtr& searchCache = util::SearchCachePartitionWrapperPtr());
    ~OfflinePartitionReader() {}

public:
    void Open(const index_base::PartitionDataPtr& partitionData) override;

    IndexPartitionReaderPtr GetSubPartitionReader() const override { return IndexPartitionReaderPtr(); }

    bool GetSortedDocIdRanges(const std::vector<std::shared_ptr<table::DimensionDescription>>& dimensions,
                              const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const override
    {
        assert(false);
        return false;
    }

    const index::SummaryReaderPtr& GetSummaryReader() const override;

    const index::SourceReaderPtr& GetSourceReader() const override;

    const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const override;

    std::shared_ptr<index::InvertedIndexReader> GetInvertedIndexReader() const override;
    const std::shared_ptr<index::InvertedIndexReader>&
    GetInvertedIndexReader(const std::string& indexName) const override;
    const std::shared_ptr<index::InvertedIndexReader>& GetInvertedIndexReader(indexid_t indexId) const override;

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override;

    const index::KKVReaderPtr& GetKKVReader(const std::string& regionName) const override;

    const index::KKVReaderPtr& GetKKVReader(regionid_t regionId = DEFAULT_REGIONID) const override;

    const index::KVReaderPtr& GetKVReader(const std::string& regionName) const override;

    const index::KVReaderPtr& GetKVReader(regionid_t regionId = DEFAULT_REGIONID) const override;

protected:
    virtual void InitAttributeReaders(config::ReadPreference readPreference, bool needPackAttributeReaders,
                                      const OnlinePartitionReader* hintPartReader) override;

private:
    const index::AttributeReaderPtr& InitOneFieldAttributeReader(const std::string& field);
    const std::shared_ptr<index::InvertedIndexReader>& InitOneIndexReader(const std::string& indexName);

private:
    mutable autil::RecursiveThreadMutex mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflinePartitionReader);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OFFLINE_PARTITION_READER_H
