#ifndef __INDEXLIB_DEFAULT_ATTRIBUTE_FIELD_APPENDER_H
#define __INDEXLIB_DEFAULT_ATTRIBUTE_FIELD_APPENDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(common, AttributeValueInitializer);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(index, InMemorySegmentReader);

IE_NAMESPACE_BEGIN(index);

class DefaultAttributeFieldAppender
{
public:
    typedef std::vector<common::AttributeValueInitializerPtr> AttrInitializerVector;

public:
    DefaultAttributeFieldAppender();
    ~DefaultAttributeFieldAppender();
    
public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const InMemorySegmentReaderPtr& inMemSegmentReader);

    void AppendDefaultFieldValues(const document::NormalDocumentPtr& document);

private:
    void InitAttributeValueInitializers(
            const config::AttributeSchemaPtr& attrSchema,
            const InMemorySegmentReaderPtr& inMemSegmentReader);

    void InitEmptyFields(const document::NormalDocumentPtr& document, 
                         const config::AttributeSchemaPtr& attrSchema,
                         bool isVirtual);

private:
    config::IndexPartitionSchemaPtr mSchema;
    AttrInitializerVector mAttrInitializers;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultAttributeFieldAppender);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEFAULT_ATTRIBUTE_FIELD_APPENDER_H
