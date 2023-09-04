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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeIndexerOrganizerUtil.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/UpdateFieldExtractor.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/IndexerOrganizerMeta.h"
#include "indexlib/index/common/IndexerOrganizerUtil.h"
#include "indexlib/index/primary_key/Common.h"

namespace indexlibv2::index {
class AttributeDiskIndexer;
class AttributeMemIndexer;
} // namespace indexlibv2::index

namespace indexlib::index {
template <typename DiskIndexerType, typename MemIndexerType>
class SingleAttributeBuilder : public autil::NoCopyable
{
public:
    SingleAttributeBuilder(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);
    virtual ~SingleAttributeBuilder();

public:
    Status Init(const indexlibv2::framework::TabletData& tabletData,
                const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig, bool isOnline);

public:
    Status AddDocument(indexlibv2::document::IDocument* doc);
    Status UpdateDocument(indexlibv2::document::IDocument* doc);

public:
    std::string GetIndexName() const;

public:
    bool ShouldSkipBuild() const;

protected:
    Status InitBuildInfoHolder(const indexlibv2::framework::TabletData& tabletData,
                               const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig, bool isOnline,
                               SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder);

private:
    // Handles config related initialization that requires cast indexConfig to AttributeConfig.
    // This is introduced because indexConfig might be casted to VirtualAttributeConfig which has a different index
    // type than AttributeConfig.
    virtual Status InitConfigRelated(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);

protected:
    const std::shared_ptr<indexlibv2::config::ITabletSchema> _schema = nullptr;
    indexlibv2::index::UpdateFieldExtractor _extractor;
    IndexerOrganizerMeta _indexerOrganizerMeta;
    SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType> _buildInfoHolder;

    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.table, SingleAttributeBuilder, T1, T2);

template <typename DiskIndexerType, typename MemIndexerType>
SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::SingleAttributeBuilder(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
    : _schema(schema)
{
}

template <typename DiskIndexerType, typename MemIndexerType>
SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::~SingleAttributeBuilder()
{
}

template <typename DiskIndexerType, typename MemIndexerType>
Status SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::InitConfigRelated(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    const auto& pkConfigs = _schema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    std::shared_ptr<indexlibv2::config::IIndexConfig> pkConfig;
    if (pkConfigs.size() > 0) {
        pkConfig = pkConfigs[0];
    }
    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> attrConfigs;
    attrConfigs.push_back(indexConfig);

    auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
    if (attributeConfig == nullptr) {
        return Status::InvalidArgs("Invalid indexConfig, name: %s", indexConfig->GetIndexName().c_str());
    }
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> fieldConfigs = {attributeConfig->GetFieldConfig()};
    _extractor.Init(pkConfig, attrConfigs, fieldConfigs);

    return AttributeIndexerOrganizerUtil::CreateSingleAttributeBuildInfoHolder(indexConfig, attributeConfig,
                                                                               &_buildInfoHolder);
}

template <typename DiskIndexerType, typename MemIndexerType>
Status SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::InitBuildInfoHolder(
    const indexlibv2::framework::TabletData& tabletData,
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig, bool isOnline,
    SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder)
{
    docid_t baseDocId = 0;
    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        indexlibv2::framework::Segment* segment = it->get();
        auto segStatus = segment->GetSegmentStatus();
        size_t docCount = segment->GetSegmentInfo()->docCount;
        std::pair<docid_t, uint32_t> key = std::make_pair(baseDocId, docCount);
        baseDocId += docCount;
        // Lazily init disk indexer. Current segment will load data once Segment::GetIndexer() is called.
        if (segStatus == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT) {
            if (!isOnline) {
                AUTIL_LOG(INFO, "Skip updating attribute for disk segment in offline build");
                continue;
            }
            if (docCount == 0) {
                continue;
            }
            _buildInfoHolder.diskIndexers[key] = nullptr;
            _buildInfoHolder.diskSegments[key] = segment;
            continue;
        }

        std::shared_ptr<MemIndexerType> dumpingMemIndexer = nullptr;
        std::shared_ptr<MemIndexerType> buildingMemIndexer = nullptr;
        Status st = IndexerOrganizerUtil::GetIndexer<DiskIndexerType, MemIndexerType>(
            segment, indexConfig, /*diskIndexer=*/nullptr, &dumpingMemIndexer, &buildingMemIndexer);
        RETURN_STATUS_DIRECTLY_IF_ERROR(st);
        if (dumpingMemIndexer == nullptr && buildingMemIndexer == nullptr) {
            continue;
        }
        if (segStatus == indexlibv2::framework::Segment::SegmentStatus::ST_DUMPING) {
            _buildInfoHolder.dumpingIndexers[key] = dumpingMemIndexer;
        } else if (indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING == segStatus) {
            _buildInfoHolder.buildingIndexer = buildingMemIndexer;
        } else {
            assert(false);
        }
    }
    return Status::OK();
}

template <typename DiskIndexerType, typename MemIndexerType>
Status SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::Init(
    const indexlibv2::framework::TabletData& tabletData,
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig, bool isOnline)
{
    IndexerOrganizerMeta::InitIndexerOrganizerMeta(tabletData, &_indexerOrganizerMeta);

    RETURN_STATUS_DIRECTLY_IF_ERROR(InitConfigRelated(indexConfig));

    return InitBuildInfoHolder(tabletData, indexConfig, isOnline, &_buildInfoHolder);
}

template <typename DiskIndexerType, typename MemIndexerType>
bool SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::ShouldSkipBuild() const
{
    return _buildInfoHolder.diskIndexers.empty() && _buildInfoHolder.dumpingIndexers.empty() &&
           _buildInfoHolder.buildingIndexer == nullptr;
}

template <typename DiskIndexerType, typename MemIndexerType>
Status SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::AddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    return _buildInfoHolder.buildingIndexer->AddDocument(doc);
}

template <typename DiskIndexerType, typename MemIndexerType>
Status SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::UpdateDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == UPDATE_FIELD);
    auto normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (normalDoc == nullptr) {
        return Status::OK();
    }
    auto attributeDocument = normalDoc->GetAttributeDocument();
    if (attributeDocument == nullptr) {
        return Status::OK();
    }
    auto docId = attributeDocument->GetDocId();
    if (INVALID_DOCID == docId) {
        return Status::OK();
    }
    bool isNull = false;
    autil::StringView value;
    if (!_extractor.GetFieldValue(attributeDocument, _buildInfoHolder.attributeConfig->GetFieldId(), &isNull, &value)) {
        return Status::OK();
    }
    if (isNull) {
        AttributeIndexerOrganizerUtil::UpdateField(docId, _indexerOrganizerMeta, &_buildInfoHolder,
                                                   autil::StringView::empty_instance(), /*isNull=*/true);
    } else {
        indexlibv2::index::AttrValueMeta meta = _buildInfoHolder.attributeConvertor->Decode(value);
        AttributeIndexerOrganizerUtil::UpdateField(docId, _indexerOrganizerMeta, &_buildInfoHolder, meta.data,
                                                   /*isNull=*/false);
    }

    return Status::OK();
}

template <typename DiskIndexerType, typename MemIndexerType>
std::string SingleAttributeBuilder<DiskIndexerType, MemIndexerType>::GetIndexName() const
{
    return _buildInfoHolder.indexConfig->GetIndexName();
}

} // namespace indexlib::index
