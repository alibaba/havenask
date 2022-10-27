#ifndef __INDEXLIB_ATTRIBUTE_TEST_UTIL_H
#define __INDEXLIB_ATTRIBUTE_TEST_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"

IE_NAMESPACE_BEGIN(index);

class AttributeTestUtil
{
public:
    template<typename T>
    static config::AttributeConfigPtr CreateAttrConfig(
            bool isMultiValue = false, bool isCompressType = false)
    {
        return CreateAttrConfig(common::TypeInfo<T>::GetFieldType(), isMultiValue,
                                "attr_name", 0, isCompressType);
    }

    static config::PackageIndexConfigPtr MakePackageIndexConfig(
            const std::string& indexName,
            uint32_t maxFieldCount,
            uint32_t bigFieldIdBase = 0, 
            bool hasSectionAttribute = true);

    static config::IndexPartitionSchemaPtr MakeSchemaWithPackageIndexConfig(
            const std::string& indexName,
            uint32_t maxFieldCount,
	    IndexType indexType = it_pack,
            uint32_t bigFieldIdBase = 0, 
            bool hasSectionAttribute = true);

    static void CreateDocCounts(uint32_t segmentCount, 
                                std::vector<uint32_t>& docCounts);

    static void CreateEmptyDelSets(const std::vector<uint32_t>& docCounts, 
                                   std::vector<std::set<docid_t> >& delDocIdSets);

    static void CreateDelSets(const std::vector<uint32_t>& docCounts, 
                                   std::vector<std::set<docid_t> >& delDocIdSets);

    static config::AttributeConfigPtr CreateAttrConfig(
            FieldType fieldType, bool isMultiValue, const std::string& fieldName, 
            fieldid_t fid, bool isCompressValue = false);

    static config::AttributeConfigPtr CreateAttrConfig(
            FieldType fieldType, bool isMultiValue, const std::string& fieldName,
            fieldid_t fid, const std::string& compressTypeStr, int32_t fixedLen = -1);

    static common::AttributeConvertorPtr CreateAttributeConvertor(
            config::AttributeConfigPtr attrConfig);
};

DEFINE_SHARED_PTR(AttributeTestUtil);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_TEST_UTIL_H
