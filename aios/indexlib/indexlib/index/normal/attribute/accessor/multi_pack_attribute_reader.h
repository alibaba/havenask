#ifndef __INDEXLIB_MULTI_PACK_ATTRIBUTE_READER_H
#define __INDEXLIB_MULTI_PACK_ATTRIBUTE_READER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class MultiPackAttributeReader
{
public:
    MultiPackAttributeReader(
        const config::AttributeSchemaPtr& attributeSchema,
        AttributeMetrics* attributeMetrics = NULL);
    
    ~MultiPackAttributeReader();
public:
    void Open(const index_base::PartitionDataPtr& partitionData);
    const PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const;
    const PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const;

private:
    typedef std::tr1::unordered_map<std::string, size_t> NameToIdxMap;
    config::AttributeSchemaPtr mAttrSchema;
    AttributeMetrics* mAttributeMetrics;
    std::vector<PackAttributeReaderPtr> mPackAttrReaders;
    NameToIdxMap mPackNameToIdxMap;
    static PackAttributeReaderPtr RET_EMPTY_PTR;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPackAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_PACK_ATTRIBUTE_READER_H
