#include "indexlib/index/inverted_index/truncate/DocFilterProcessorTyped.h"

#include "indexlib/index/inverted_index/truncate/test/MockHelper.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DocFilterProcessorTypedTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(DocFilterProcessorTypedTest, TestBeginFilter)
{
    auto attrReader = std::make_shared<MockTruncateAttributeReader>();
    auto ctx = std::make_shared<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>();
    EXPECT_CALL(*attrReader, CreateReadContextPtr(_)).WillRepeatedly(Return(ctx));

    indexlibv2::config::DiversityConstrain constrain;
    DictKeyInfo key;
    auto obj = std::make_unique<DocFilterProcessorTyped<int>>(attrReader, constrain);
    ASSERT_TRUE(obj->BeginFilter(key, /*postingIt*/ nullptr));

    int64_t min = 10;
    int64_t max = 100;
    auto metaReader = std::make_shared<MockTruncateMetaReader>();
    EXPECT_CALL(*metaReader, Lookup(key, _, _))
        .WillOnce(DoAll(::testing::SetArgReferee<1>(min), ::testing::SetArgReferee<2>(max), Return(false)))
        .WillOnce(DoAll(::testing::SetArgReferee<1>(min), ::testing::SetArgReferee<2>(max), Return(true)));
    obj.reset(new DocFilterProcessorTyped<int>(attrReader, constrain));
    obj->SetTruncateMetaReader(metaReader);
    ASSERT_FALSE(obj->BeginFilter(key, /*postingIt*/ nullptr));
    ASSERT_TRUE(obj->BeginFilter(key, /*postingIt*/ nullptr));
    ASSERT_EQ(10, obj->_minValue);
    ASSERT_EQ(100, obj->_maxValue);
}

TEST_F(DocFilterProcessorTypedTest, TestIsFiltered)
{
    auto attrReader = std::make_shared<MockTruncateAttributeReader>();
    auto ctx = std::make_shared<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>();
    EXPECT_CALL(*attrReader, CreateReadContextPtr(_)).WillRepeatedly(Return(ctx));

    indexlibv2::config::DiversityConstrain constrain;
    auto obj = std::make_unique<DocFilterProcessorTyped<uint8_t>>(attrReader, constrain);
    obj->_minValue = 10;
    obj->_maxValue = 100;
    obj->_mask = 0xFFFF;

    bool isNull = true;
    bool isNotNull = false;
    uint8_t small = 4;
    uint8_t mid1 = 10;  // equal to min value
    uint8_t mid2 = 20;  // in the mid
    uint8_t mid3 = 100; // equal to max value
    uint8_t big = 200;

    EXPECT_CALL(*attrReader, Read(_, _, _, _, _, _))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(small), ::testing::SetArgReferee<4>(isNull), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(small), ::testing::SetArgReferee<4>(isNotNull), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mid1), ::testing::SetArgReferee<4>(isNotNull), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mid2), ::testing::SetArgReferee<4>(isNotNull), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mid3), ::testing::SetArgReferee<4>(isNotNull), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(big), ::testing::SetArgReferee<4>(isNotNull), Return(true)));

    docid_t docId = 1;
    // is null, return true directory
    ASSERT_TRUE(obj->IsFiltered(docId));
    // val < min value
    ASSERT_TRUE(obj->IsFiltered(docId));
    // min value <= val <= max value
    ASSERT_FALSE(obj->IsFiltered(docId));
    ASSERT_FALSE(obj->IsFiltered(docId));
    ASSERT_FALSE(obj->IsFiltered(docId));
    // val > max value
    ASSERT_TRUE(obj->IsFiltered(docId));
}

} // namespace indexlib::index
