#ifndef __INDEXLIB_MULTI_REGION_PACK_ATTRIBUTE_APPENDER_H
#define __INDEXLIB_MULTI_REGION_PACK_ATTRIBUTE_APPENDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/pack_attribute_appender.h"

IE_NAMESPACE_BEGIN(document);

class MultiRegionPackAttributeAppender
{
public:
    MultiRegionPackAttributeAppender();
    ~MultiRegionPackAttributeAppender();
    
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema);
    
    bool AppendPackAttribute(
        const document::AttributeDocumentPtr& attrDocument,
        autil::mem_pool::Pool* pool, regionid_t regionId);

private:
    std::vector<PackAttributeAppenderPtr> mAppenders;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionPackAttributeAppender);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_MULTI_REGION_PACK_ATTRIBUTE_APPENDER_H
