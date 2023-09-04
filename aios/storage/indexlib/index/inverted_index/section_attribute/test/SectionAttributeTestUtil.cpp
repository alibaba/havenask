#include "indexlib/index/inverted_index/section_attribute/test/SectionAttributeTestUtil.h"

#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"

namespace indexlib::index {
std::shared_ptr<indexlibv2::config::ITabletSchema>
SectionAttributeTestUtil::MakeSchemaWithPackageIndexConfig(const std::string& indexName, uint32_t maxFieldCount,
                                                           InvertedIndexType indexType, uint32_t bigFieldIdBase,
                                                           bool hasSectionAttribute)
{
    std::string fieldStr = "";
    uint32_t i;
    char buf[8];
    for (i = 0; i < maxFieldCount + bigFieldIdBase; ++i) {
        fieldStr += "text" + std::to_string(i) + ":text;";
    }
    fieldStr += "pkstring:string;"; // primary key field
    std::string indexStr = "pk:primarykey64:pkstring;";
    std::string packIndexStr = indexName;
    if (indexType == it_pack) {
        packIndexStr += ":pack:";
    } else {
        assert(indexType == it_expack);
        packIndexStr += ":expack:";
    }
    for (i = 0; i < maxFieldCount; ++i) {
        snprintf(buf, sizeof(buf), "text%d", bigFieldIdBase + i);
        std::string fieldName = buf;
        packIndexStr += fieldName + ",";
    }
    if (i > 0) {
        packIndexStr[packIndexStr.size() - 1] = ';';
    }

    if (packIndexStr != indexName) {
        indexStr += packIndexStr;
    }

    auto schema = indexlibv2::table::NormalTabletSchemaMaker::Make(fieldStr, indexStr, "", "");
    assert(schema);
    if (packIndexStr != indexName) {
        auto packConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
            schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, indexName));
        assert(packConfig);
        packConfig->SetHasSectionAttributeFlag(hasSectionAttribute);
    }
    return schema;
}
} // namespace indexlib::index
