#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/truncate/AttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/DocInfo.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/DocPayloadEvaluator.h"
#include "indexlib/index/inverted_index/truncate/MultiAttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/ReferenceTyped.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"
#include "indexlib/index/inverted_index/truncate/test/MockHelper.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class EvaluatorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(EvaluatorTest, TestSimple)
{
    auto postingIter = std::make_shared<MockPostingIterator>();
    auto diskIndexer = std::make_shared<MockTruncateAttributeReader>();

    auto allocator = std::make_unique<DocInfoAllocator>();
    auto docPayloadRefer = allocator->DeclareReference(/*fieldName*/ "DOC_PAYLOAD", ft_double, /*supportNull*/ false);
    auto attrRefer = allocator->DeclareReference(/*fieldName*/ "ut_field", ft_uint8, /*supportNull*/ true);
    auto docInfo = allocator->Allocate();

    uint8_t val = 20;
    auto ctx = std::make_shared<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>();
    EXPECT_CALL(*diskIndexer, Read(_, _, _, _, _, _))
        .WillRepeatedly(DoAll(::testing::SetArgPointee<2>(val), ::testing::SetArgReferee<4>(false), Return(true)));
    EXPECT_CALL(*diskIndexer, CreateReadContextPtr(_)).WillRepeatedly(Return(ctx));

    auto docPayloadEvaluator = std::make_shared<DocPayloadEvaluator>(docPayloadRefer);
    auto attrEvaluator = std::make_shared<AttributeEvaluator<uint8_t>>(diskIndexer, attrRefer);
    auto multiAttrEvaluator = std::make_shared<MultiAttributeEvaluator>();
    multiAttrEvaluator->AddEvaluator(docPayloadEvaluator);
    multiAttrEvaluator->AddEvaluator(attrEvaluator);

    EXPECT_CALL(*postingIter, SeekDoc(/*docId*/ 0)).WillRepeatedly(Return(/*docId*/ 0));
    EXPECT_CALL(*postingIter, GetDocPayload()).WillRepeatedly(Return(/*doc payload*/ 10));

    multiAttrEvaluator->Evaluate(/*docId*/ 0, postingIter, docInfo);

    ASSERT_EQ(std::string("10"), docPayloadRefer->GetStringValue(docInfo));
    ASSERT_EQ(std::string("20"), attrRefer->GetStringValue(docInfo));
}

} // namespace indexlib::index
