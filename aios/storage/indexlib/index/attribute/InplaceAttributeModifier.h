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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeIndexerOrganizerUtil.h"
#include "indexlib/index/attribute/AttributeModifier.h"

namespace indexlibv2::index {
class AttributeDiskIndexer;
class AttributeMemIndexer;
class AttributeConfig;

class InplaceAttributeModifier : public AttributeModifier
{
public:
    InplaceAttributeModifier(const std::shared_ptr<config::ITabletSchema>& schema);
    ~InplaceAttributeModifier() = default;

public:
    Status Init(const framework::TabletData& tabletData) override;
    bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) override;

    Status Update(document::IDocumentBatch* docBatch);
    bool UpdateAttrDoc(docid_t docId, indexlib::document::AttributeDocument* attrDoc);
    bool UpdateAttribute(docid_t docId,
                         indexlib::index::SingleAttributeBuildInfoHolder<index::AttributeDiskIndexer,
                                                                         index::AttributeMemIndexer>* buildInfoHolder,
                         const autil::StringView& value, const bool isNull);

private:
    Status GetIndexerForAttributeConfig(indexlibv2::framework::Segment* segment,
                                        std::shared_ptr<AttributeConfig> attributeConfig,
                                        std::shared_ptr<AttributeDiskIndexer>* diskIndexer,
                                        std::shared_ptr<AttributeMemIndexer>* dumpingMemIndexer,
                                        std::shared_ptr<AttributeMemIndexer>* buildingMemIndexer);
    void InitDiskIndexer(const std::pair<docid_t, uint64_t>& baseDocId2DocCnt,
                         const std::shared_ptr<AttributeDiskIndexer>& diskIndexer, fieldid_t fieldId);
    void InitMemIndexer(const std::pair<docid_t, uint64_t>& baseDocId2DocCnt,
                        const std::shared_ptr<AttributeMemIndexer>& memIndexer, fieldid_t fieldId);
    static void ValidateNullValue(
        const std::map<fieldid_t, indexlib::index::SingleAttributeBuildInfoHolder<
                                      index::AttributeDiskIndexer, index::AttributeMemIndexer>>& buildInfoHolders);

private:
    indexlib::index::IndexerOrganizerMeta _indexerOrganizerMeta;

    std::map<fieldid_t,
             indexlib::index::SingleAttributeBuildInfoHolder<index::AttributeDiskIndexer, index::AttributeMemIndexer>>
        _buildInfoHolders;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
