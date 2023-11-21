#pragma once
#include "indexlib/index/attribute/test/AttributeTestUtil.h"

namespace indexlib::index {
class SectionAttributeTestUtil : public indexlibv2::index::AttributeTestUtil
{
public:
    static std::shared_ptr<indexlibv2::config::ITabletSchema>
    MakeSchemaWithPackageIndexConfig(const std::string& indexName, uint32_t maxFieldCount, InvertedIndexType indexType,
                                     uint32_t bigFieldIdBase, bool hasSectionAttribute);
};
} // namespace indexlib::index
