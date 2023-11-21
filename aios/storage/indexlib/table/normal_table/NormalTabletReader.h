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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/proto/query.pb.h"
#include "indexlib/framework/TabletReader.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index/field_meta/FieldMetaReader.h"

namespace indexlib::util {
class AccumulativeCounter;
} // namespace indexlib::util

namespace indexlib::index {
class InvertedIndexReader;
class PrimaryKeyIndexReader;
class MultiFieldIndexReader;
class FieldMetaReader;
class IndexAccessoryReader;
} // namespace indexlib::index

namespace indexlib::table {
struct DimensionDescription;
} // namespace indexlib::table

namespace indexlibv2::index {
class DeletionMapIndexReader;
class AttributeReader;
class PackAttributeReader;
class SummaryReader;
class SourceReader;
struct DiskIndexerParameter;
struct IndexReaderParameter;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class ITabletSchema;
class InvertedIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::table {

class NormalTabletInfoHolder;
class NormalTabletInfo;
class NormalTabletMeta;
class SortedDocIdRangeSearcher;
class NormalTabletMetrics;
class NormalTabletReader : public framework::TabletReader
{
public:
    NormalTabletReader(const std::shared_ptr<config::ITabletSchema>& schema,
                       const std::shared_ptr<NormalTabletMetrics>& normalTabletMetrics);
    ~NormalTabletReader() = default;

public:
    Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                  const framework::ReadResource& readResource) final override;

    // redirect to NormalTabletSearcher::Search
    Status Search(const std::string& jsonQuery, std::string& result) const final override;

public:
    std::shared_ptr<NormalTabletInfo> GetNormalTabletInfo() const;
    const framework::Version& GetVersion() const;

    std::shared_ptr<indexlib::index::InvertedIndexReader> GetMultiFieldIndexReader() const;
    const std::shared_ptr<index::DeletionMapIndexReader>& GetDeletionMapReader() const;
    const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& GetPrimaryKeyReader() const;
    const std::shared_ptr<indexlib::index::FieldMetaReader> GetFieldMetaReader(const std::string& fieldName) const;
    const std::shared_ptr<std::map<std::string, std::shared_ptr<indexlib::index::FieldMetaReader>>>&
    GetFieldMetaReadersMap()
    {
        return _fieldMetaIndexFieldNameToReader;
    }
    std::shared_ptr<index::SummaryReader> GetSummaryReader() const;
    std::shared_ptr<index::SourceReader> GetSourceReader() const;
    std::shared_ptr<index::AttributeReader> GetAttributeReader(const std::string& attrName) const;

    std::shared_ptr<index::PackAttributeReader> GetPackAttributeReader(const std::string& packName) const;

    bool GetSortedDocIdRanges(const std::vector<std::shared_ptr<indexlib::table::DimensionDescription>>& dimensions,
                              const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const;
    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount, size_t wayIdx,
                              DocIdRangeVector& ranges) const;
    bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                              std::vector<DocIdRangeVector>& rangeVectors) const;

    typedef std::unordered_map<std::string, std::shared_ptr<indexlib::util::AccumulativeCounter>> AccessCounterMap;
    const AccessCounterMap& GetInvertedAccessCounter() const;
    const AccessCounterMap& GetAttributeAccessCounter() const;

    Status QueryIndex(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse) const;
    Status QueryDocIds(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse,
                       std::vector<docid_t>& docids) const;

private:
    Status InitSummaryReader();
    Status InitInvertedIndexReaders();
    Status InitIndexAccessoryReader(const framework::TabletData* tabletData,
                                    const framework::ReadResource& readResource);
    Status InitSortedDocidRangeSearcher();
    Status InitNormalTabletInfoHolder(const std::shared_ptr<framework::TabletData>& tabletData,
                                      const framework::ReadResource& readResource);
    template <typename T>
    void AddAttributeReader(const std::shared_ptr<indexlib::index::InvertedIndexReader>& indexReader,
                            const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);
    Status PrepareIndexReaderParameter(const framework::ReadResource& readResource,
                                       index::IndexReaderParameter& parameter);
    void InitFieldMetaIndexFieldNameToReader();

private:
    std::shared_ptr<indexlib::index::MultiFieldIndexReader> _multiFieldIndexReader;
    std::shared_ptr<indexlib::index::IndexAccessoryReader> _indexAccessoryReader;
    std::shared_ptr<NormalTabletInfoHolder> _normalTabletInfoHolder;
    std::shared_ptr<NormalTabletMeta> _normalTabletMeta;
    framework::Version _version;
    std::shared_ptr<SortedDocIdRangeSearcher> _sortedDocIdRangeSearcher;
    std::shared_ptr<indexlib::index::PrimaryKeyIndexReader> _primaryKeyIndexReader;
    std::shared_ptr<index::DeletionMapIndexReader> _deletionMapReader;
    std::shared_ptr<NormalTabletMetrics> _normalTabletMetrics;
    std::shared_ptr<std::map<std::string, std::shared_ptr<indexlib::index::FieldMetaReader>>>
        _fieldMetaIndexFieldNameToReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
