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
#ifndef __INDEXLIB_INPLACE_ATTRIBUTE_MODIFIER_H
#define __INDEXLIB_INPLACE_ATTRIBUTE_MODIFIER_H

#include <memory>

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderContainer);

namespace indexlib { namespace index {

class InplaceAttributeModifier : public AttributeModifier
{
public:
    InplaceAttributeModifier(const config::IndexPartitionSchemaPtr& schema,
                             util::BuildResourceMetrics* buildResourceMetrics);

    ~InplaceAttributeModifier();

public:
    void Init(const AttributeReaderContainerPtr& attrReaderContainer,
              const index_base::PartitionDataPtr& partitionData);
    bool Update(docid_t docId, const document::AttributeDocumentPtr& attrDoc) override;
    // must be thread-safe
    void UpdateAttribute(docid_t docId, const document::AttributeDocumentPtr& attrDoc, attrid_t attrId) override;
    // must be thread-safe
    void UpdatePackAttribute(docid_t docId, const document::AttributeDocumentPtr& attrDoc,
                             packattrid_t packAttrId) override;

    bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) override;
    bool UpdatePackField(docid_t docId, packattrid_t packAttrId, const autil::StringView& value);

    void Dump(const file_system::DirectoryPtr& dir);

    // for AttributePatchWorkItem
    AttributeSegmentReaderPtr GetAttributeSegmentReader(fieldid_t fieldId, docid_t segBaseDocId) const;

    // for PackFieldPatchWorkItem and batch build AttributeBuildWorkItem
    PackAttributeReaderPtr GetPackAttributeReader(packattrid_t packAttrId) const;

    // for batch build AttributeBuildWorkItem
    AttributeReaderPtr GetAttributeReader(const config::AttributeConfigPtr& attrConfig);

private:
    void UpdateInPackFields(docid_t docId);
    void UpdateInPackField(docid_t docId, packattrid_t packAttrId);

private:
    typedef std::vector<AttributeReaderPtr> AttributeReaderMap;
    typedef std::vector<PackAttributeReaderPtr> PackAttributeReaderMap;
    typedef std::vector<common::PackAttributeFormatter::PackAttributeFields> PackAttrFieldsVector;

private:
    AttributeReaderMap _attributeReaderMap;
    PackAttributeReaderMap _packAttributeReaderMap;
    PackAttrFieldsVector _packIdToPackFields;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InplaceAttributeModifier);
}} // namespace indexlib::index

#endif //__INDEXLIB_INPLACE_ATTRIBUTE_MODIFIER_H
