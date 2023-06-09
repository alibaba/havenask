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

#include <map>
#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/DefaultValueAttributeMemReader.h"
#include "indexlib/index/attribute/MultiSliceAttributeDiskIndexer.h"
#include "indexlib/index/attribute/RangeDescription.h"
#include "indexlib/index/common/field_format/attribute/AttributeFieldPrinter.h"

namespace indexlib { namespace index {
class DeletionMapReader;
}} // namespace indexlib::index

namespace indexlibv2::index {
class AttributeIteratorBase;

enum PatchApplyStrategy : unsigned int;
#define DECLARE_ATTRIBUTE_READER_IDENTIFIER(id)                                                                        \
    static std::string Identifier() { return std::string("attribute.reader." #id); }                                   \
    virtual std::string GetIdentifier() const override { return Identifier(); }

class AttributeReader : public IIndexReader
{
public:
    using Segments = std::vector<framework::Segment*>;
    using AttributeReaderPtr = std::shared_ptr<AttributeReader>;
    using IndexerInfo = std::tuple<std::shared_ptr<IIndexer>, std::shared_ptr<framework::Segment>, docid_t>;
    static constexpr std::string_view DEFAULT_VALUE_READER = "defaultValueReader";

public:
    explicit AttributeReader(indexlibv2::config::SortPattern sortType)
        : _enableAccessCountors(false)
        , _sortType(sortType)
        , _isIntegrated(true)
        , _fillDefaultAttrReader(false)
    {
    }
    virtual ~AttributeReader() = default;

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;

    Status OpenWithSegments(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                            std::vector<std::shared_ptr<framework::Segment>> segments);

public:
    virtual bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const = 0;
    virtual AttrType GetType() const = 0;
    virtual bool IsMultiValue() const = 0;
    virtual std::string GetIdentifier() const = 0;
    virtual AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool* pool) const = 0;
    virtual std::unique_ptr<AttributeIteratorBase> CreateSequentialIterator() const = 0;
    virtual bool GetSortedDocIdRange(const indexlib::index::RangeDescription& range, const DocIdRange& rangeLimit,
                                     DocIdRange& resultRange) const = 0;
    virtual std::string GetAttributeName() const = 0;
    virtual size_t EstimateLoadSize(const Segments& allSegments,
                                    const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                    const framework::Version& lastLoadVersion)
    {
        // compatible with PrimaryKey interface(PrimaryKeyAttributeReader)
        assert(false);
        return 0;
    }

    virtual std::shared_ptr<AttributeDiskIndexer> GetIndexer(docid_t docId) const = 0;

    virtual void EnableAccessCountors() = 0;
    virtual void EnableGlobalReadContext() = 0;

protected:
    virtual Status DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                          const std::vector<IndexerInfo>& indexers) = 0;

    template <typename DiskIndexer, typename MemIndexer, typename MemReader>
    Status InitAllIndexers(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                           const std::vector<IndexerInfo>& indexers,
                           std::vector<std::shared_ptr<DiskIndexer>>& diskIndexers,
                           std::vector<std::pair<docid_t, std::shared_ptr<MemReader>>>& memReaders,
                           std::shared_ptr<DefaultValueAttributeMemReader>& defaultValueReader);

    template <typename MemIndexer, typename MemReader>
    Status InitBuildingAttributeReader(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                                       const std::vector<IndexerInfo>& indexers,
                                       std::vector<std::pair<docid_t, std::shared_ptr<MemReader>>>& memReaders);

protected:
    bool _enableAccessCountors;
    config::SortPattern _sortType;

    AttributeFieldPrinter _fieldPrinter;
    std::vector<uint64_t> _segmentDocCount;
    std::shared_ptr<config::AttributeConfig> _attrConfig;
    bool _isIntegrated;
    bool _fillDefaultAttrReader;
    bool _enableGlobalContext = false;

private:
    AUTIL_LOG_DECLARE();
};

template <typename DiskIndexer, typename MemIndexer, typename MemReader>
inline Status AttributeReader::InitAllIndexers(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                                               const std::vector<IndexerInfo>& indexers,
                                               std::vector<std::shared_ptr<DiskIndexer>>& diskIndexers,
                                               std::vector<std::pair<docid_t, std::shared_ptr<MemReader>>>& memReaders,
                                               std::shared_ptr<DefaultValueAttributeMemReader>& defaultValueReader)
{
    _attrConfig = attrConfig;
    _fieldPrinter.Init(attrConfig->GetFieldConfig());
    std::vector<IndexerInfo> memIndexers;
    for (auto& [indexer, segment, baseDocId] : indexers) {
        auto docCount = segment->GetSegmentInfo()->docCount;
        auto segStatus = segment->GetSegmentStatus();
        auto segId = segment->GetSegmentId();

        if (framework::Segment::SegmentStatus::ST_BUILT == segStatus) {
            if (indexer == nullptr) {
                auto [status, diskIndexer] =
                    AttributeDefaultDiskIndexerFactory::CreateDefaultDiskIndexer(segment, attrConfig);
                RETURN_IF_STATUS_ERROR(status, "create default indexer for attr [%s] in segment [%d] failed",
                                       attrConfig->GetIndexName().c_str(), segId);
                segment->AddIndexer(ATTRIBUTE_INDEX_TYPE_STR, attrConfig->GetIndexName(), diskIndexer);
                auto useIndexer = std::dynamic_pointer_cast<DiskIndexer>(diskIndexer);
                AUTIL_LOG(INFO, "add default value attribute reader [%s] for segment [%d]",
                          attrConfig->GetIndexName().c_str(), segId);
                _segmentDocCount.emplace_back(docCount);
                diskIndexers.emplace_back(useIndexer);
                continue;
            }
            auto multiSliceDiskIndexer = std::dynamic_pointer_cast<MultiSliceAttributeDiskIndexer>(indexer);
            if (!multiSliceDiskIndexer) {
                AUTIL_LOG(ERROR, "no indexer for index [%s] segment [%d]", attrConfig->GetIndexName().c_str(), segId);
                return Status::InternalError("indexer for %s in segment [%d] with on OnDiskIndexer",
                                             attrConfig->GetIndexName().c_str(), segId);
            }

            for (int32_t i = 0; i < multiSliceDiskIndexer->GetSliceCount(); i++) {
                auto diskIndexer = multiSliceDiskIndexer->template GetSliceIndexer<DiskIndexer>(i);
                if (!diskIndexer) {
                    AUTIL_LOG(ERROR, "no indexer for index [%s] segment [%d]", attrConfig->GetIndexName().c_str(),
                              segId);
                    return Status::InternalError("indexer for %s in segment [%d] with on OnDiskIndexer",
                                                 attrConfig->GetIndexName().c_str(), segId);
                }
                _isIntegrated = _isIntegrated && (diskIndexer->GetBaseAddress() != nullptr);

                auto sliceDocCount = multiSliceDiskIndexer->GetSliceDocCount(i);
                _segmentDocCount.emplace_back(sliceDocCount);
                diskIndexers.emplace_back(diskIndexer);
            }
        } else if (framework::Segment::SegmentStatus::ST_DUMPING == segStatus ||
                   framework::Segment::SegmentStatus::ST_BUILDING == segStatus) {
            memIndexers.emplace_back(indexer, segment, baseDocId);
            continue;
        }
    }

    auto st = InitBuildingAttributeReader<MemIndexer, MemReader>(attrConfig, memIndexers, memReaders);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "init building attr reader failed, error[%s]", st.ToString().c_str());
        return st;
    }

    if (_fillDefaultAttrReader) {
        defaultValueReader.reset(new DefaultValueAttributeMemReader());
        auto status = defaultValueReader->Open(attrConfig);
        RETURN_IF_STATUS_ERROR(status, "open default value attribute mem reader [%s] failed",
                               attrConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

template <typename MemIndexer, typename MemReader>
inline Status
AttributeReader::InitBuildingAttributeReader(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                                             const std::vector<IndexerInfo>& indexers,
                                             std::vector<std::pair<docid_t, std::shared_ptr<MemReader>>>& memReaders)
{
    for (auto& [indexer, segment, baseDocId] : indexers) {
        auto memIndexer = std::dynamic_pointer_cast<MemIndexer>(indexer);
        if (memIndexer) {
            auto memReader = std::dynamic_pointer_cast<MemReader>(memIndexer->CreateInMemReader());
            assert(memReader);
            memReaders.emplace_back(std::make_pair(baseDocId, memReader));
        } else {
            return Status::InternalError("indexer for [%s] in segment[%d] has no mem indexer",
                                         attrConfig->GetIndexName().c_str(), segment->GetSegmentId());
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::index
