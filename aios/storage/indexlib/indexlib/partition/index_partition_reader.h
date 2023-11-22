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
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_version.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(index, LegacyIndexReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderContainer);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index, KKVReader);
DECLARE_REFERENCE_CLASS(index, KVReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(index, SourceReader);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

namespace indexlibv2::framework {
struct VersionDeployDescription;
};
namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlib { namespace partition {

typedef std::shared_ptr<IndexPartitionReader> IndexPartitionReaderPtr;
class IndexPartitionReader
{
public:
    typedef util::AccessCounterMap AccessCounterMap;
    static constexpr uint32_t MAIN_PART_ID = 0;
    static constexpr uint32_t SUB_PART_ID = 1;

public:
    IndexPartitionReader();
    IndexPartitionReader(config::IndexPartitionSchemaPtr schema);
    virtual ~IndexPartitionReader();

public:
    virtual void Open(const index_base::PartitionDataPtr& partitionData) = 0;

    virtual const index::SummaryReaderPtr& GetSummaryReader() const = 0;
    virtual const index::SourceReaderPtr& GetSourceReader() const = 0;
    virtual const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const = 0;
    virtual const index::PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const = 0;
    virtual const index::PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const = 0;
    virtual const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const = 0;

    virtual index::InvertedIndexReaderPtr GetInvertedIndexReader() const = 0;
    virtual const index::InvertedIndexReaderPtr& GetInvertedIndexReader(const std::string& indexName) const = 0;
    virtual const index::InvertedIndexReaderPtr& GetInvertedIndexReader(indexid_t indexId) const = 0;

    virtual const index::DeletionMapReaderPtr& GetDeletionMapReader() const = 0;

    virtual const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const = 0;

    virtual const index::KKVReaderPtr& GetKKVReader(regionid_t regionId = DEFAULT_REGIONID) const;
    virtual const index::KKVReaderPtr& GetKKVReader(const std::string& regionName) const;

    virtual const index::KVReaderPtr& GetKVReader(regionid_t regionId = DEFAULT_REGIONID) const;
    virtual const index::KVReaderPtr& GetKVReader(const std::string& regionName) const;

    virtual const table::TableReaderPtr& GetTableReader() const;

    virtual index_base::Version GetVersion() const = 0;
    virtual index_base::Version GetOnDiskVersion() const = 0;

    virtual index::PartitionInfoPtr GetPartitionInfo() const = 0;

    virtual IndexPartitionReaderPtr GetSubPartitionReader() const = 0;

    virtual bool GetSortedDocIdRanges(const std::vector<std::shared_ptr<table::DimensionDescription>>& dimensions,
                                      const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const = 0;

    virtual bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount, size_t wayIdx,
                                      DocIdRangeVector& ranges) const;
    virtual bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                                      std::vector<DocIdRangeVector>& rangeVectors) const;

    virtual const AccessCounterMap& GetIndexAccessCounters() const = 0;

    virtual const AccessCounterMap& GetAttributeAccessCounters() const = 0;
    config::IndexPartitionSchemaPtr& GetSchema() { return mSchema; }
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& GetTabletSchema() { return mTabletSchema; }
    virtual versionid_t GetIncVersionId() const;

    virtual index_base::PartitionDataPtr GetPartitionData() const;

    virtual bool IsUsingSegment(segmentid_t segmentId) const;

    const PartitionVersion* GetPartitionVersion() const { return mPartitionVersion.get(); }

    uint64_t GetPartitionVersionHashId() const { return mPartitionVersionHashId; }

    virtual int64_t GetLatestDataTimestamp() const;

    const std::vector<segmentid_t>& GetSwitchRtSegments() const { return mSwitchRtSegIds; }

    virtual std::shared_ptr<indexlibv2::framework::VersionDeployDescription> GetVersionDeployDescription() const;

public:
    void TEST_SetPartitionVersion(std::unique_ptr<PartitionVersion> partitionVersion);

protected:
    virtual void InitPartitionVersion(const index_base::PartitionDataPtr& partitionData, segmentid_t lastLinkSegId);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    std::shared_ptr<indexlibv2::config::ITabletSchema> mTabletSchema;
    uint64_t mPartitionVersionHashId;
    std::unique_ptr<PartitionVersion> mPartitionVersion;
    std::vector<segmentid_t> mSwitchRtSegIds;

protected:
    static index::SummaryReaderPtr RET_EMPTY_SUMMARY_READER;
    static index::SourceReaderPtr RET_EMPTY_SOURCE_READER;
    static index::PrimaryKeyIndexReaderPtr RET_EMPTY_PRIMARY_KEY_READER;
    static index::DeletionMapReaderPtr RET_EMPTY_DELETIONMAP_READER;
    static index::InvertedIndexReaderPtr RET_EMPTY_INDEX_READER;
    static index::KKVReaderPtr RET_EMPTY_KKV_READER;
    static index::KVReaderPtr RET_EMPTY_KV_READER;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
