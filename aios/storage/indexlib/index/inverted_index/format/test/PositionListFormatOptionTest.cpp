#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class PositionListFormatOptionTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    void InnerTestInitPositionListMeta(optionflag_t optionFlag, bool expectHasPosList, bool expectHasPosPayload,
                                       bool expectHasTfBitmap)
    {
        PositionListFormatOption posListFormatOption(optionFlag);
        ASSERT_EQ(expectHasPosList, posListFormatOption.HasPositionList());
        ASSERT_EQ(expectHasPosPayload, posListFormatOption.HasPositionPayload());
        ASSERT_EQ(expectHasTfBitmap, posListFormatOption.HasTfBitmap());
    }
};

TEST_F(PositionListFormatOptionTest, testSimpleProcess)
{
    PositionListFormatOption posListFormatOption(EXPACK_OPTION_FLAG_ALL);

    ASSERT_TRUE(posListFormatOption.HasPositionList());
    ASSERT_TRUE(posListFormatOption.HasPositionPayload());
    ASSERT_TRUE(!posListFormatOption.HasTfBitmap());
}

TEST_F(PositionListFormatOptionTest, testInit)
{
    InnerTestInitPositionListMeta(/*optionFlag=*/OPTION_FLAG_ALL,
                                  /*expectHasPosList=*/true,
                                  /*expectHasPosPayload=*/true,
                                  /*expectHasTfBitmap=*/false);

    // test position and position_payload
    InnerTestInitPositionListMeta(of_position_list | of_position_payload, true, true, false);
    InnerTestInitPositionListMeta(of_position_list, true, false, false);
    InnerTestInitPositionListMeta(of_position_payload, false, false, false);

    // test tf and tf_bitmap
    InnerTestInitPositionListMeta(of_position_payload | of_position_list | of_term_frequency | of_tf_bitmap, true, true,
                                  true);
}

TEST_F(PositionListFormatOptionTest, testJsonize)
{
    PositionListFormatOption option(NO_TERM_FREQUENCY);
    JsonizablePositionListFormatOption jsonizableOption(option);
    std::string jsonStr = autil::legacy::ToJsonString(jsonizableOption);
    JsonizablePositionListFormatOption convertedJsonizableOption;
    autil::legacy::FromJsonString(convertedJsonizableOption, jsonStr);
    PositionListFormatOption convertedOption = convertedJsonizableOption.GetPositionListFormatOption();
    ASSERT_EQ(option, convertedOption);
}

} // namespace indexlib::index
