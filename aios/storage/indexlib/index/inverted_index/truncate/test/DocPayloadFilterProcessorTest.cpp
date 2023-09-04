#include "indexlib/index/inverted_index/truncate/DocPayloadFilterProcessor.h"

#include "indexlib/index/inverted_index/truncate/test/MockHelper.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DocPayloadFilterProcessorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(DocPayloadFilterProcessorTest, TestIsFiltered)
{
    indexlibv2::config::DiversityConstrain constrain;
    auto obj = std::make_unique<DocPayloadFilterProcessor>(constrain, /*evaluator*/ nullptr, /*allocator*/ nullptr);
    obj->_minValue = 10;
    obj->_maxValue = 100;
    obj->_mask = 0xFFFF;

    uint16_t small = 4;
    uint16_t mid1 = 10;  // equal to min value
    uint16_t mid2 = 20;  // in the mid
    uint16_t mid3 = 100; // equal to max value
    uint16_t big = 200;

    auto postingIter = std::make_shared<MockPostingIterator>();
    EXPECT_CALL(*postingIter, GetDocPayload())
        .WillOnce(Return(small))
        .WillOnce(Return(mid1))
        .WillOnce(Return(mid2))
        .WillOnce(Return(mid3))
        .WillOnce(Return(big));

    DictKeyInfo key;
    obj->BeginFilter(key, postingIter);
    docid_t docId = 1;
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
