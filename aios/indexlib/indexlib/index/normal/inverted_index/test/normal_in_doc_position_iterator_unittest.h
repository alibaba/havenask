#include <string>
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_posting_maker.h"
#include "indexlib/index/test/payload_checker.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/section_data_maker.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"

IE_NAMESPACE_BEGIN(index);

class NormalInDocPositionIteratorTest : public INDEXLIB_TESTBASE 
{
public:
    typedef IndexTestUtil::AnswerMap AnswerMap;

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForSectionFeature();
    void TestCaseForNoPayloadSectionFeature();
    void TestCaseForNoPositionListSectionFeature();
    void TestCaseForTfBitmap();
    void TestCaseForRepeatInit();

    DECLARE_CLASS_NAME(NormalInDocPositionIteratorTest);

private:
    void TestForSectionFeature(optionflag_t optionFlag);
    SectionAttributeReaderImplPtr PrepareSectionReader();

private:
    static const uint32_t MAX_FIELD_COUNT = 32;
    std::string mDir;
    config::PackageIndexConfigPtr mIndexConfig;
    config::IndexPartitionSchemaPtr mSchema;
};

INDEXLIB_UNIT_TEST_CASE(NormalInDocPositionIteratorTest, TestCaseForSectionFeature);
INDEXLIB_UNIT_TEST_CASE(NormalInDocPositionIteratorTest, TestCaseForNoPayloadSectionFeature);
INDEXLIB_UNIT_TEST_CASE(NormalInDocPositionIteratorTest, TestCaseForNoPositionListSectionFeature);
INDEXLIB_UNIT_TEST_CASE(NormalInDocPositionIteratorTest, TestCaseForTfBitmap);
// INDEXLIB_UNIT_TEST_CASE(NormalInDocPositionIteratorTest, TestCaseForRepeatInit);

IE_NAMESPACE_END(index);
