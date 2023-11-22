#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/document/document_rewriter/section_attribute_appender.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class SectionAttributeAppenderTest : public INDEXLIB_TESTBASE
{
public:
    SectionAttributeAppenderTest();
    ~SectionAttributeAppenderTest();

    DECLARE_CLASS_NAME(SectionAttributeAppenderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAppendSectionAttrWithDifferentParam();

private:
    void InnerTestAppendSectionAttribute(const std::string& despStr, const std::string& indexFilter,
                                         bool isUniqCompress, bool hasSectionWeight, bool hasFieldId);

    void ResetSchema();
    void OverwriteSchema(const config::IndexPartitionSchemaPtr& schema, const std::string& indexFilter,
                         bool isUniqCompress, bool hasSectionWeight, bool hasFieldId);

    document::IndexDocumentPtr CreateIndexDocument(const std::string& despStr);

    void InnerCheckDocument(const document::IndexDocumentPtr& indexDocument, const std::string& despStr);

    void CheckSectionAttribute(const autil::StringView& sectionAttr,
                               const config::PackageIndexConfigPtr& packIndexConfig, const std::string& despStr);

    void CheckUnpackSectionAttribute(section_len_t* lengthBuf, section_fid_t* fidBuf, section_weight_t* weightBuf,
                                     uint32_t sectionCount, const std::vector<int>& sectionCountsPerField,
                                     const config::PackageIndexConfigPtr& packIndexConfig);

private:
    config::IndexPartitionSchemaPtr mSchema;
    autil::mem_pool::Pool* mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SectionAttributeAppenderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeAppenderTest, TestAppendSectionAttrWithDifferentParam);
}} // namespace indexlib::document
