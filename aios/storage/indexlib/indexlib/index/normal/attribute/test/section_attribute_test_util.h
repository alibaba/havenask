#ifndef __INDEXLIB_SECTION_ATTRIBUTE_TEST_UTIL_H
#define __INDEXLIB_SECTION_ATTRIBUTE_TEST_UTIL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/common/field_format/section_attribute/SectionMeta.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SectionAttributeTestUtil
{
public:
    typedef std::map<docid_t, std::vector<SectionMeta>> Answer;

public:
    SectionAttributeTestUtil();
    ~SectionAttributeTestUtil();

public:
    static void BuildMultiSegmentsData(const file_system::DirectoryPtr& rootDirectory,
                                       const config::IndexPartitionSchemaPtr& schema, const std::string& indexName,
                                       const std::vector<uint32_t>& docCounts, Answer& answer);

    static void BuildOneSegmentData(const file_system::DirectoryPtr& rootDirectory, segmentid_t segId,
                                    const config::IndexPartitionSchemaPtr& schema, const std::string& indexName,
                                    uint32_t docCount, Answer& answer);

    static void WriteSegmentInfo(const file_system::DirectoryPtr& directory, uint32_t docCount);

private:
    static const uint32_t MAX_FIELD_COUNT = 32;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeTestUtil);
}} // namespace indexlib::index

#endif //__INDEXLIB_SECTION_ATTRIBUTE_TEST_UTIL_H
