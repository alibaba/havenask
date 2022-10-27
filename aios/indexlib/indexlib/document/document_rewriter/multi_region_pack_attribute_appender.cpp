#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, MultiRegionPackAttributeAppender);

MultiRegionPackAttributeAppender::MultiRegionPackAttributeAppender() 
{
}

MultiRegionPackAttributeAppender::~MultiRegionPackAttributeAppender() 
{
}

bool MultiRegionPackAttributeAppender::Init(const IndexPartitionSchemaPtr& schema)
{
    if (!schema)
    {
        return false;
    }
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++)
    {
        PackAttributeAppenderPtr appender(new PackAttributeAppender);
        if (!appender->Init(schema, id))
        {
            return false;
        }
        mAppenders.push_back(appender);
    }
    return true;
}

bool MultiRegionPackAttributeAppender::AppendPackAttribute(
        const AttributeDocumentPtr& attrDocument,
        Pool* pool, regionid_t regionId)
{
    if (regionId >= (regionid_t)mAppenders.size())
    {
        return false;
    }
    return mAppenders[regionId]->AppendPackAttribute(attrDocument, pool);
}

IE_NAMESPACE_END(document);

