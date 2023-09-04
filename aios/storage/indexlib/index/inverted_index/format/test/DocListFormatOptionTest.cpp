#include "indexlib/index/inverted_index/format/DocListFormatOption.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class DocListFormatOptionTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    void InnerTestInitDocListMeta(optionflag_t optionFlag, bool expectHasTf, bool expectHasTfList,
                                  bool expectHasTfBitmap, bool expectHasDocPayload, bool expectHasFieldMap)
    {
        DocListFormatOption formatOption(optionFlag);
        ASSERT_EQ(expectHasTf, formatOption.HasTermFrequency());
        ASSERT_EQ(expectHasTfList, formatOption.HasTfList());
        ASSERT_EQ(expectHasTfBitmap, formatOption.HasTfBitmap());
        ASSERT_EQ(expectHasDocPayload, formatOption.HasDocPayload());
        ASSERT_EQ(expectHasFieldMap, formatOption.HasFieldMap());
    }
};

TEST_F(DocListFormatOptionTest, TestSimpleProcess)
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    ASSERT_TRUE(docListFormatOption.HasTermFrequency());
    ASSERT_TRUE(docListFormatOption.HasTfList());
    ASSERT_TRUE(!docListFormatOption.HasTfBitmap());
    ASSERT_TRUE(docListFormatOption.HasDocPayload());
    ASSERT_TRUE(docListFormatOption.HasFieldMap());
}

TEST_F(DocListFormatOptionTest, testCopyConstructor)
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormatOption newDocListFormatOption = docListFormatOption;
    ASSERT_TRUE(docListFormatOption.HasTermFrequency());
    ASSERT_TRUE(newDocListFormatOption.HasTfList());
    ASSERT_TRUE(!newDocListFormatOption.HasTfBitmap());
    ASSERT_TRUE(newDocListFormatOption.HasDocPayload());
    ASSERT_TRUE(newDocListFormatOption.HasFieldMap());
}

TEST_F(DocListFormatOptionTest, testInit)
{
    InnerTestInitDocListMeta(
        /*optionFlag=*/OPTION_FLAG_ALL,
        /*expectHasTf=*/true,
        /*expectHasTfList=*/true,
        /*expectHasTfBitmap=*/false,
        /*expectHasDocPayload=*/true,
        /*expectHasFieldMap=*/false);

    optionflag_t optionFlag = of_doc_payload | of_term_frequency | of_tf_bitmap;
    InnerTestInitDocListMeta(optionFlag, true, false, true, true, false);

    optionFlag = of_doc_payload | of_tf_bitmap;
    InnerTestInitDocListMeta(optionFlag, false, false, false, true, false);

    optionFlag = of_doc_payload | of_term_frequency;
    InnerTestInitDocListMeta(optionFlag, true, true, false, true, false);

    optionFlag = of_doc_payload | of_term_frequency | of_fieldmap;
    InnerTestInitDocListMeta(optionFlag, true, true, false, true, true);
}

TEST_F(DocListFormatOptionTest, testJsonize)
{
    DocListFormatOption option(NO_POSITION_LIST);
    JsonizableDocListFormatOption jsonizableOption(option);
    std::string jsonStr = autil::legacy::ToJsonString(jsonizableOption);
    JsonizableDocListFormatOption convertedJsonizableOption;
    autil::legacy::FromJsonString(convertedJsonizableOption, jsonStr);
    DocListFormatOption convertedOption = convertedJsonizableOption.GetDocListFormatOption();
    ASSERT_EQ(option, convertedOption);
}

} // namespace indexlib::index
