#ifndef __INDEXLIB_SECTION_ATTRIBUTE_TEST_UTIL_H
#define __INDEXLIB_SECTION_ATTRIBUTE_TEST_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_writer.h"
#include "indexlib/common/field_format/section_attribute/section_meta.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(index);

class SectionAttributeTestUtil
{
public:
    typedef std::map<docid_t, std::vector<common::SectionMeta> > Answer;

public:
    SectionAttributeTestUtil();
    ~SectionAttributeTestUtil();

public:
    static void BuildMultiSegmentsData(
            const file_system::DirectoryPtr& rootDirectory, 
	    const config::IndexPartitionSchemaPtr& schema,
	    const std::string& indexName,
            const std::vector<uint32_t>& docCounts, Answer& answer);

    static void BuildOneSegmentData(
            const file_system::DirectoryPtr& rootDirectory,
            segmentid_t segId,
	    const config::IndexPartitionSchemaPtr& schema,
	    const std::string& indexName,
            uint32_t docCount, Answer& answer);


    static void WriteSegmentInfo(const file_system::DirectoryPtr& directory, 
                                 uint32_t docCount);

private:
    static const uint32_t MAX_FIELD_COUNT = 32;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeTestUtil);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_TEST_UTIL_H
