#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SectionDataMaker
{
public:
    static void BuildOneSegmentSectionData(const config::IndexPartitionSchemaPtr& schema, const std::string& indexName,
                                           const std::string& sectionStr, const file_system::DirectoryPtr& dir,
                                           segmentid_t segId);

    static void BuildMultiSegmentsSectionData(const config::IndexPartitionSchemaPtr& schema,
                                              const std::string& indexName, const std::vector<std::string>& sectionStrs,
                                              const file_system::DirectoryPtr& dir);

    static std::string CreateFakeSectionString(docid_t maxDocId, pos_t maxPos);

    static void CreateFakeSectionString(docid_t maxDocId, pos_t maxPos, const std::vector<std::string>& strs,
                                        std::vector<std::string>& sectStrs);

private:
    static void CreateFieldsFromDocSectionString(const std::string& docSectionString,
                                                 std::vector<document::Field*>& fields, autil::mem_pool::Pool* pool);

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionDataMaker);
}} // namespace indexlib::index
