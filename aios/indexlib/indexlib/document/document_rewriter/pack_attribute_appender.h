#ifndef __INDEXLIB_PACK_ATTRIBUTE_APPENDER_H
#define __INDEXLIB_PACK_ATTRIBUTE_APPENDER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(document);

class PackAttributeAppender
{
public:
    typedef std::vector<common::PackAttributeFormatterPtr> FormatterVector;
public:
    PackAttributeAppender() {}
    ~PackAttributeAppender() {}
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema,
              regionid_t regionId = DEFAULT_REGIONID);
    
    bool AppendPackAttribute(
        const document::AttributeDocumentPtr& attrDocument,
        autil::mem_pool::Pool* pool);

private:
    bool InitOnePackAttr(const config::PackAttributeConfigPtr& packAttrConfig);
    bool CheckPackAttrFields(const document::AttributeDocumentPtr& attrDocument);

private:
    FormatterVector mPackFormatters;
    std::vector<fieldid_t> mInPackFields;
    std::vector<fieldid_t> mClearFields;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeAppender);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_PACK_ATTRIBUTE_APPENDER_H
