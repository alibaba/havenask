#ifndef __INDEXLIB_INPLACE_ATTRIBUTE_MODIFIER_H
#define __INDEXLIB_INPLACE_ATTRIBUTE_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderContainer);

IE_NAMESPACE_BEGIN(index);

class InplaceAttributeModifier : public AttributeModifier
{
public:
    InplaceAttributeModifier(
            const config::IndexPartitionSchemaPtr& schema,
            util::BuildResourceMetrics* buildResourceMetrics);
    
    ~InplaceAttributeModifier();
public:
    void Init(const AttributeReaderContainerPtr& attrReaderContainer,
              const index_base::PartitionDataPtr& partitionData);
    bool Update(
            docid_t docId, const document::AttributeDocumentPtr& attrDoc) override;
    bool UpdateField(
            docid_t docId, fieldid_t fieldId, 
            const autil::ConstString& value) override;

    bool UpdatePackField(docid_t docId, packattrid_t packAttrId, 
                         const autil::ConstString& value);
    
    void Dump(const file_system::DirectoryPtr& dir);

    // for AttributePatchWorkItem
    AttributeSegmentReaderPtr GetAttributeSegmentReader(
        fieldid_t fieldId, docid_t segBaseDocId) const;

    // for PackFieldPatchWorkItem
    PackAttributeReaderPtr GetPackAttributeReader(packattrid_t packAttrId) const;
    
private:
    void UpdateInPackFields(docid_t docId);
    
private:
    typedef std::vector<AttributeReaderPtr> AttributeModifierMap;
    typedef std::vector<PackAttributeReaderPtr> PackAttributeModifierMap;
    typedef std::vector<common::PackAttributeFormatter::PackAttributeFields> PackAttrFieldsVector;

private:
    AttributeModifierMap mModifierMap;
    PackAttributeModifierMap mPackModifierMap;
    PackAttrFieldsVector mPackIdToPackFields;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InplaceAttributeModifier);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INPLACE_ATTRIBUTE_MODIFIER_H
