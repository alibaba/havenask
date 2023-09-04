#include "indexlib/index/operation_log/UpdateFieldOperation.h"

#include "future_lite/coro/Lazy.h"
#include "indexlib/index/operation_log/OperationLogProcessor.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "unittest/unittest.h"

namespace indexlib::index {

namespace {

template <typename T>
class MockPrimaryKeyIndexReader : public indexlibv2::index::PrimaryKeyReader<T>
{
public:
    MockPrimaryKeyIndexReader() : indexlibv2::index::PrimaryKeyReader<T>(nullptr) {}

public:
    MOCK_METHOD(docid_t, LookupWithDocRange,
                (const autil::uint128_t&, (std::pair<docid_t, docid_t>), future_lite::Executor*), (const, override));
};

class MockModifier : public OperationLogProcessor
{
public:
    MockModifier() = default;
    ~MockModifier() = default;

    MOCK_METHOD(Status, RemoveDocument, (docid_t docid), (override));
    MOCK_METHOD(bool, UpdateFieldValue, (docid_t, const std::string&, const autil::StringView&, bool), (override));
    MOCK_METHOD(Status, UpdateFieldTokens, (docid_t, const document::ModifiedTokens&), (override));
};
}; // namespace

class UpdateFieldOperationTest : public TESTBASE
{
public:
    UpdateFieldOperationTest() = default;
    ~UpdateFieldOperationTest() = default;
    void setUp() override;
    void tearDown() override {}

private:
    template <typename T>
    void InnerTestProcess(bool isNull);

    template <typename T>
    std::shared_ptr<UpdateFieldOperation<T>> MakeOperation(segmentid_t segmentId, bool isNull);

private:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<UpdateFieldOperation<uint64_t>> _operation;
    fieldid_t _fieldId = 3;
    int64_t _ts = 9;
    uint64_t _pkHash = 12345;
    uint32_t _itemSize = 2;
    uint16_t _hashId = 4;
    std::shared_ptr<OperationFieldInfo> _fieldInfo;
};

void UpdateFieldOperationTest::setUp()
{
    _fieldInfo.reset(new OperationFieldInfo());
    _fieldInfo->TEST_AddField(_fieldId, "3");
    _operation = MakeOperation<uint64_t>(/*segId*/ 10, /*isNull*/ false);
}

template <typename T>
std::shared_ptr<UpdateFieldOperation<T>> UpdateFieldOperationTest::MakeOperation(segmentid_t segmentId, bool isNull)
{
    std::string value = isNull ? "" : "abc";
    autil::mem_pool::Pool* pool = &_pool;
    OperationItem* items = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, _itemSize);
    items[0].first = _fieldId;
    items[0].second = autil::MakeCString(value, pool);
    items[1].first = _fieldId;
    items[1].second = items[0].second;
    T hashValue(_pkHash);
    std::shared_ptr<UpdateFieldOperation<T>> operation(new UpdateFieldOperation<T>({_hashId, _ts, 0}));
    operation->Init(hashValue, items, _itemSize, segmentId);
    operation->SetOperationFieldInfo(_fieldInfo);
    return operation;
}

template <typename T>
void UpdateFieldOperationTest::InnerTestProcess(bool isNull)
{
    const segmentid_t segId = 10;
    auto op = MakeOperation<T>(segId, isNull);
    auto mockModifier = std::make_unique<MockModifier>();
    auto mockPkIndexReader = std::make_unique<MockPrimaryKeyIndexReader<T>>();

    std::vector<std::pair<docid_t, docid_t>> docIdRange({{10, 100}});

    EXPECT_CALL(*mockPkIndexReader, LookupWithDocRange(_, _, _)).WillOnce(Return(INVALID_DOCID));
    EXPECT_CALL(*mockModifier, UpdateFieldValue(_, _, _, _)).Times(0);

    ASSERT_TRUE(op->Process(mockPkIndexReader.get(), mockModifier.get(), docIdRange));
    ASSERT_EQ(segId, op->GetSegmentId());

    // case2: update normal case
    docid_t docId = 20;
    EXPECT_CALL(*mockPkIndexReader, LookupWithDocRange(_, _, _)).WillOnce(Return(docId));
    EXPECT_CALL(*mockModifier, UpdateFieldValue(docId, "3", op->_items[0].second, isNull))
        .WillOnce(Return(true))
        .WillOnce(Return(true));

    ASSERT_TRUE(op->Process(mockPkIndexReader.get(), mockModifier.get(), docIdRange));
    ASSERT_EQ(segId, op->GetSegmentId());
}

TEST_F(UpdateFieldOperationTest, TestClose)
{
    auto cloneOp = _operation->Clone(&_pool);
    ASSERT_NE(cloneOp, nullptr);
    auto cloneUpdateOp = dynamic_cast<UpdateFieldOperation<uint64_t>*>(cloneOp);
    ASSERT_NE(cloneUpdateOp, nullptr);

    ASSERT_EQ(_pkHash, cloneUpdateOp->_pkHash);
    ASSERT_EQ(_ts, cloneUpdateOp->_docInfo.timestamp);
    ASSERT_EQ(_itemSize, cloneUpdateOp->_itemSize);
    ASSERT_TRUE(_operation->_items != cloneUpdateOp->_items);
    for (size_t i = 0; i < _operation->_itemSize; ++i) {
        ASSERT_EQ(_operation->_items[i], cloneUpdateOp->_items[i]);
    }
}

TEST_F(UpdateFieldOperationTest, TestSerialize)
{
    char buffer[1024];
    // 23 means stub value for current impl
    EXPECT_EQ(23, _operation->Serialize(buffer, 1024));

    UpdateFieldOperation<uint64_t> op({_hashId, _ts, 0});
    char* cursor = buffer;
    ASSERT_TRUE(op.Load(&_pool, cursor));

    ASSERT_EQ(_operation->_pkHash, op._pkHash);
    ASSERT_EQ(_operation->_docInfo.timestamp, op._docInfo.timestamp);
    ASSERT_EQ(_operation->_segmentId, op._segmentId);
    ASSERT_EQ(_operation->_itemSize, op._itemSize);
    for (size_t i = 0; i < op._itemSize; ++i) {
        ASSERT_EQ(_operation->_items[i], op._items[i]);
    }
}

TEST_F(UpdateFieldOperationTest, TestProcess)
{
    InnerTestProcess<uint64_t>(false);
    InnerTestProcess<autil::uint128_t>(false);

    InnerTestProcess<uint64_t>(true);
    InnerTestProcess<autil::uint128_t>(true);
}

TEST_F(UpdateFieldOperationTest, TestMemoryUse)
{
    // 118 means stub value for current impl
    EXPECT_EQ(118, _operation->GetMemoryUse());
}
} // namespace indexlib::index
