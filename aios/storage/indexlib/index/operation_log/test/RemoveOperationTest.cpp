#include "indexlib/index/operation_log/RemoveOperation.h"

#include "indexlib/index/operation_log/OperationBlock.h"
#include "indexlib/index/operation_log/OperationFactory.h"
#include "indexlib/index/operation_log/OperationLogProcessor.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {
namespace {

} // namespace

class RemoveOperationTest : public INDEXLIB_TESTBASE
{
public:
    RemoveOperationTest();
    ~RemoveOperationTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestProcess();
    void TestClone();
    void TestGetMemoryUse();
    void TestSerialize();
    void TestSerializeByDumper();

private:
    template <typename T>
    void DoTestProcess();

private:
    autil::mem_pool::Pool mPool;
    std::shared_ptr<RemoveOperation<uint64_t>> mOperation;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, RemoveOperationTest);

namespace {
template <typename T>
class MockPrimaryKeyIndexReader : public indexlibv2::index::PrimaryKeyReader<T>
{
public:
    MockPrimaryKeyIndexReader() : indexlibv2::index::PrimaryKeyReader<T>(nullptr) {}

public:
    MOCK_METHOD(docid64_t, LookupWithDocRange,
                (const autil::uint128_t&, (std::pair<docid_t, docid_t>), future_lite::Executor*), (const, override));
};

class MockOperationLogProcessor : public OperationLogProcessor
{
public:
    MOCK_METHOD(Status, RemoveDocument, (docid_t), (override));
    MOCK_METHOD(bool, UpdateFieldValue, (docid_t, const std::string&, const autil::StringView&, bool), (override));
    MOCK_METHOD(Status, UpdateFieldTokens, (docid_t, const document::ModifiedTokens&), (override));
};

// class MockPatchModifier : public partition::PatchModifier
// {
// public:
//     MockPatchModifier(const config::IndexPartitionSchemaPtr& schema) : PatchModifier(schema) {}

//     MOCK_METHOD(index::PrimaryKeyIndexReaderPtr, LoadPrimaryKeyIndexReader, (const index_base::PartitionDataPtr&),
//     (override)); MOCK_METHOD(bool, RemoveDocument, (docid_t), (override)); MOCK_METHOD(index::PartitionInfoPtr,
//     GetPartitionInfo, (), (const, override));
// };
}; // namespace

RemoveOperationTest::RemoveOperationTest() {}

RemoveOperationTest::~RemoveOperationTest() {}

void RemoveOperationTest::CaseSetUp()
{
    mPool.allocate(8); // first allocate will create header(use more memory)

    uint64_t hashValue(12345);
    mOperation.reset(new RemoveOperation<uint64_t>({100, 10, 0, 0}));
    mOperation->Init(hashValue, segmentid_t(20));
}

void RemoveOperationTest::CaseTearDown() {}

template <typename T>
void RemoveOperationTest::DoTestProcess()
{
    std::shared_ptr<RemoveOperation<T>> operationPtr(new RemoveOperation<T>({100, 10, 0, 0}));
    T hashValue(12345);
    operationPtr->Init(hashValue, 1);
    std::shared_ptr<MockPrimaryKeyIndexReader<T>> pkIndexReader(new MockPrimaryKeyIndexReader<T>());
    std::shared_ptr<MockOperationLogProcessor> processor(new MockOperationLogProcessor);
    EXPECT_CALL(*processor, RemoveDocument(_)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*pkIndexReader, LookupWithDocRange(_, _, _)).WillOnce(Return(INVALID_DOCID)).WillOnce(Return(10));
    std::vector<std::pair<docid_t, docid_t>> docIdRange;
    docIdRange.push_back({0, 100});
    operationPtr->Process(pkIndexReader.get(), processor.get(), docIdRange);
    operationPtr->Process(pkIndexReader.get(), processor.get(), docIdRange);
    ASSERT_EQ(1u, operationPtr->GetSegmentId());
}

TEST_F(RemoveOperationTest, TestProcess)
{
    DoTestProcess<uint64_t>();
    DoTestProcess<autil::uint128_t>();
}

TEST_F(RemoveOperationTest, TestClone)
{
    OperationBase* clonedOperation = mOperation->Clone(&mPool);
    RemoveOperation<uint64_t>* clonedRemoveOperation = dynamic_cast<RemoveOperation<uint64_t>*>(clonedOperation);
    ASSERT_TRUE(clonedRemoveOperation);

    uint64_t expectHashValue(12345);
    ASSERT_EQ(expectHashValue, clonedRemoveOperation->_pkHash);
    ASSERT_EQ((int64_t)10, clonedRemoveOperation->_docInfo.timestamp);
    ASSERT_EQ((int64_t)100, clonedRemoveOperation->_docInfo.hashId);
    ASSERT_EQ((segmentid_t)20, clonedRemoveOperation->_segmentId);
}

TEST_F(RemoveOperationTest, TestGetMemoryUse)
{
    size_t memUseBegin = mPool.getUsedBytes();
    OperationBase* clonedOperation = mOperation->Clone(&mPool);
    size_t memUseEnd = mPool.getUsedBytes();
    ASSERT_EQ(memUseEnd - memUseBegin, clonedOperation->GetMemoryUse());
}

TEST_F(RemoveOperationTest, TestSerialize)
{
    char buffer[1024];
    mOperation->Serialize(buffer, 1024);

    autil::mem_pool::Pool pool;
    RemoveOperation<uint64_t> operation({100, 10, 0, 0});
    char* cursor = buffer;
    operation.Load(&pool, cursor);

    ASSERT_EQ(mOperation->_pkHash, operation._pkHash);
    ASSERT_EQ(mOperation->_docInfo.timestamp, operation._docInfo.timestamp);
    ASSERT_EQ(mOperation->_docInfo.hashId, operation._docInfo.hashId);
    ASSERT_EQ(mOperation->_segmentId, operation._segmentId);
}

TEST_F(RemoveOperationTest, TestSerializeByDumper)
{
    char buffer[1024];
    [[maybe_unused]] size_t bufferLen =
        OperationBlock::DumpSingleOperation(mOperation.get(), buffer, 1024, false, false);
    OperationFactory opFactory;
    opFactory._mainPkType = it_primarykey64;
    autil::mem_pool::Pool pool;
    size_t opSize = 0;
    auto [status, op] = opFactory.DeserializeOperation(buffer, &pool, opSize, false, false);
    ASSERT_TRUE(status.IsOK());
    auto castOp = dynamic_cast<RemoveOperation<uint64_t>*>(op);
    ASSERT_TRUE(castOp);
    ASSERT_EQ(mOperation->_pkHash, castOp->_pkHash);
    ASSERT_EQ(mOperation->_docInfo.timestamp, castOp->_docInfo.timestamp);
    ASSERT_EQ(mOperation->_docInfo.hashId, castOp->_docInfo.hashId);
    ASSERT_EQ(mOperation->_segmentId, castOp->_segmentId);
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, op);
}

} // namespace indexlib::index
