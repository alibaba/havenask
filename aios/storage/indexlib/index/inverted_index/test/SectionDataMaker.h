#pragma once
#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlib::file_system {
class Directory;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::document {
class Field;
}

namespace indexlibv2::index {

class SectionDataMaker
{
public:
    static void BuildOneSegmentSectionData(const std::shared_ptr<config::ITabletSchema>& schema,
                                           const std::string& indexName, const std::string& sectionStr,
                                           const std::shared_ptr<indexlib::file_system::Directory>& dir,
                                           segmentid_t segId);

    static void BuildMultiSegmentsSectionData(const std::shared_ptr<config::ITabletSchema>& schema,
                                              const std::string& indexName, const std::vector<std::string>& sectionStrs,
                                              const std::shared_ptr<indexlib::file_system::Directory>& dir);

    static std::string CreateFakeSectionString(docid_t maxDocId, pos_t maxPos);

    static void CreateFakeSectionString(docid_t maxDocId, pos_t maxPos, const std::vector<std::string>& strs,
                                        std::vector<std::string>& sectStrs);

private:
    static void CreateFieldsFromDocSectionString(const std::string& docSectionString,
                                                 std::vector<indexlib::document::Field*>& fields,
                                                 autil::mem_pool::Pool* pool);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
