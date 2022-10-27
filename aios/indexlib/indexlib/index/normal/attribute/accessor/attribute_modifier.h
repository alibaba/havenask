#ifndef __INDEXLIB_ATTRIBUTE_MODIFIER_H
#define __INDEXLIB_ATTRIBUTE_MODIFIER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, AttributeUpdateBitmap);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

class AttributeModifier
{
public:
    AttributeModifier(const config::IndexPartitionSchemaPtr& schema,
                      util::BuildResourceMetrics* buildResourceMetrics);
    virtual ~AttributeModifier();

public:
    virtual bool Update(
            docid_t docId, const document::AttributeDocumentPtr& attrDoc) = 0;
    virtual bool UpdateField(docid_t docId, fieldid_t fieldId, 
                             const autil::ConstString& value) = 0;

private:
    void InitAttributeConvertorMap(
            const config::IndexPartitionSchemaPtr& schema);

protected:
    void InitPackAttributeUpdateBitmap(
        const index_base::PartitionDataPtr& partitionData);
    
    void DumpPackAttributeUpdateInfo(
            const file_system::DirectoryPtr& attrDirectory) const;
    
protected:
    typedef std::vector<common::AttributeConvertorPtr> FieldIdToConvertorMap;
    typedef std::vector<AttributeUpdateBitmapPtr> PackAttrUpdateBitmapVec;

    config::IndexPartitionSchemaPtr mSchema;
    FieldIdToConvertorMap mFieldId2ConvertorMap;
    PackAttrUpdateBitmapVec mPackUpdateBitmapVec;
    util::BuildResourceMetrics* mBuildResourceMetrics; 
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeModifier);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_MODIFIER_H
