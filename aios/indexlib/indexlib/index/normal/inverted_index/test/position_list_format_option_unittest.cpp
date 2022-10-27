#include "indexlib/index/normal/inverted_index/test/position_list_format_option_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListFormatOptionTest);

PositionListFormatOptionTest::PositionListFormatOptionTest()
{
}

PositionListFormatOptionTest::~PositionListFormatOptionTest()
{
}

void PositionListFormatOptionTest::SetUp()
{
}

void PositionListFormatOptionTest::TearDown()
{
}

void PositionListFormatOptionTest::TestSimpleProcess()
{
    PositionListFormatOption posListFormatOption(EXPACK_OPTION_FLAG_ALL);
    
    ASSERT_EQ(true, posListFormatOption.HasPositionList());
    ASSERT_EQ(true, posListFormatOption.HasPositionPayload());
    ASSERT_EQ(false, posListFormatOption.HasTfBitmap());
}

void PositionListFormatOptionTest::TestInit()
{
    InnerTestInitPositionListMeta(/*optionFlag=*/OPTION_FLAG_ALL,
                                  /*expectHasPosList=*/true,
                                  /*expectHasPosPayload=*/true,
                                  /*expectHasTfBitmap=*/false);

    //test position and position_payload
    InnerTestInitPositionListMeta(of_position_list | of_position_payload,
                                  true, true, false);
    InnerTestInitPositionListMeta(of_position_list,
                                  true, false, false);
    InnerTestInitPositionListMeta(of_position_payload,
                                  false, false, false);

    //test tf and tf_bitmap
    InnerTestInitPositionListMeta(of_position_payload 
                                  | of_position_list
                                  | of_term_frequency
                                  | of_tf_bitmap,
                                  true, true, true);
}

void PositionListFormatOptionTest::InnerTestInitPositionListMeta(
        optionflag_t optionFlag, 
        bool expectHasPosList,
        bool expectHasPosPayload,
        bool expectHasTfBitmap)
{
    PositionListFormatOption posListFormatOption(optionFlag);
    ASSERT_EQ(expectHasPosList, posListFormatOption.HasPositionList());
    ASSERT_EQ(expectHasPosPayload, posListFormatOption.HasPositionPayload());
    ASSERT_EQ(expectHasTfBitmap, posListFormatOption.HasTfBitmap());
}

void PositionListFormatOptionTest::TestJsonize()
{
    PositionListFormatOption option(NO_TERM_FREQUENCY);
    JsonizablePositionListFormatOption jsonizableOption(option);
    string jsonStr = autil::legacy::ToJsonString(jsonizableOption);
    JsonizablePositionListFormatOption convertedJsonizableOption;
    autil::legacy::FromJsonString(convertedJsonizableOption, jsonStr); 
    PositionListFormatOption convertedOption = 
        convertedJsonizableOption.GetPositionListFormatOption();
    ASSERT_EQ(option, convertedOption);
}

IE_NAMESPACE_END(index);

