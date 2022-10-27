#include "indexlib/index/normal/inverted_index/test/doc_list_format_option_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocListFormatOptionTest);

DocListFormatOptionTest::DocListFormatOptionTest()
{
}

DocListFormatOptionTest::~DocListFormatOptionTest()
{
}

void DocListFormatOptionTest::SetUp()
{
}

void DocListFormatOptionTest::TearDown()
{
}

void DocListFormatOptionTest::TestSimpleProcess()
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    ASSERT_EQ(true, docListFormatOption.HasTermFrequency());
    ASSERT_EQ(true, docListFormatOption.HasTfList());
    ASSERT_EQ(false, docListFormatOption.HasTfBitmap());
    ASSERT_EQ(true, docListFormatOption.HasDocPayload());
    ASSERT_EQ(true, docListFormatOption.HasFieldMap());
}


void DocListFormatOptionTest::TestCopyConstructor()
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormatOption newDocListFormatOption = docListFormatOption;
    ASSERT_EQ(true, docListFormatOption.HasTermFrequency());
    ASSERT_EQ(true, newDocListFormatOption.HasTfList());
    ASSERT_EQ(false, newDocListFormatOption.HasTfBitmap());
    ASSERT_EQ(true, newDocListFormatOption.HasDocPayload());
    ASSERT_EQ(true, newDocListFormatOption.HasFieldMap());
}

void DocListFormatOptionTest::TestInit()
{
    InnerTestInitDocListMeta(
            /*optionFlag=*/OPTION_FLAG_ALL,
            /*expectHasTf=*/true,
            /*expectHasTfList=*/true,
            /*expectHasTfBitmap=*/false,
            /*expectHasDocPayload=*/true,
            /*expectHasFieldMap=*/false);

    optionflag_t optionFlag = of_doc_payload | of_term_frequency | of_tf_bitmap;
    InnerTestInitDocListMeta(optionFlag, true,
                             false, true, true, false);

    optionFlag = of_doc_payload | of_tf_bitmap;
    InnerTestInitDocListMeta(optionFlag, false,
                             false, false, true, false);

    optionFlag = of_doc_payload | of_term_frequency;
    InnerTestInitDocListMeta(optionFlag, true,
                             true, false, true, false);

    optionFlag = of_doc_payload | of_term_frequency | of_fieldmap;
    InnerTestInitDocListMeta(optionFlag, true,
                             true, false, true, true);
}

void DocListFormatOptionTest::InnerTestInitDocListMeta(
        optionflag_t optionFlag, bool expectHasTf, 
        bool expectHasTfList, bool expectHasTfBitmap, 
        bool expectHasDocPayload, bool expectHasFieldMap)
{
    DocListFormatOption formatOption(optionFlag);
    ASSERT_EQ(expectHasTf, formatOption.HasTermFrequency());
    ASSERT_EQ(expectHasTfList, formatOption.HasTfList());
    ASSERT_EQ(expectHasTfBitmap, formatOption.HasTfBitmap());
    ASSERT_EQ(expectHasDocPayload, formatOption.HasDocPayload());
    ASSERT_EQ(expectHasFieldMap, formatOption.HasFieldMap());
}

void DocListFormatOptionTest::TestJsonize()
{
    DocListFormatOption option(NO_POSITION_LIST);
    JsonizableDocListFormatOption jsonizableOption(option);
    string jsonStr = autil::legacy::ToJsonString(jsonizableOption);
    JsonizableDocListFormatOption convertedJsonizableOption;
    autil::legacy::FromJsonString(convertedJsonizableOption, jsonStr); 
    DocListFormatOption convertedOption = 
        convertedJsonizableOption.GetDocListFormatOption();
    ASSERT_EQ(option, convertedOption);
}

IE_NAMESPACE_END(index);

