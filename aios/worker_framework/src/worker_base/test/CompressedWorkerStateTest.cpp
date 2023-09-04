#include "worker_framework/CompressedWorkerState.h"

#include "unittest/unittest.h"
#include "worker_base/test/MockWorkerState.h"

using namespace testing;

namespace worker_framework {
namespace worker_base {

class CompressedWorkerStateTest : public testing::Test {
protected:
    template <typename RealType>
    void doTestCreate(autil::CompressType type) {
        auto underlying = std::make_unique<MockWorkerState>();
        auto state = CompressedWorkerState::create(std::move(underlying), type);
        ASSERT_TRUE(dynamic_cast<RealType *>(state.get()) != nullptr);
    }
};

TEST_F(CompressedWorkerStateTest, testCreate) {
    doTestCreate<MockWorkerState>(autil::CompressType::INVALID_COMPRESS_TYPE);
    doTestCreate<MockWorkerState>(autil::CompressType::NO_COMPRESS);
    doTestCreate<MockWorkerState>(autil::CompressType::MAX);

    doTestCreate<CompressedWorkerState>(autil::CompressType::Z_SPEED_COMPRESS);
    doTestCreate<CompressedWorkerState>(autil::CompressType::Z_DEFAULT_COMPRESS);
    doTestCreate<CompressedWorkerState>(autil::CompressType::Z_BEST_COMPRESS);
    doTestCreate<CompressedWorkerState>(autil::CompressType::SNAPPY);
    doTestCreate<CompressedWorkerState>(autil::CompressType::LZ4);
}

TEST_F(CompressedWorkerStateTest, testRead) {
    auto underlying = std::make_unique<MockWorkerState>();
    MockWorkerState *mock = underlying.get();
    std::string content;
    auto state = CompressedWorkerState::create(std::move(underlying), autil::CompressType::Z_BEST_COMPRESS);
    EXPECT_CALL(*mock, read(_)).WillOnce(Return(WorkerState::EC_NOT_EXIST));
    EXPECT_EQ(WorkerState::EC_NOT_EXIST, state->read(content));
    EXPECT_CALL(*mock, read(_)).WillOnce(Return(WorkerState::EC_FAIL));
    EXPECT_EQ(WorkerState::EC_FAIL, state->read(content));

    std::string compressedData = "invalid";
    EXPECT_CALL(*mock, read(_)).WillOnce(DoAll(SetArgReferee<0>(compressedData), Return(WorkerState::EC_OK)));
    EXPECT_EQ(WorkerState::EC_FAIL, state->read(content));

    ASSERT_TRUE(
        autil::CompressionUtil::compress(std::string("abcd"), autil::CompressType::Z_BEST_COMPRESS, compressedData));
    EXPECT_CALL(*mock, read(_)).WillOnce(DoAll(SetArgReferee<0>(compressedData), Return(WorkerState::EC_OK)));
    ASSERT_EQ(WorkerState::EC_OK, state->read(content));
    EXPECT_EQ("abcd", content);
}

TEST_F(CompressedWorkerStateTest, testWrite) {
    auto underlying = std::make_unique<MockWorkerState>();
    MockWorkerState *mock = underlying.get();
    auto state = CompressedWorkerState::create(std::move(underlying), autil::CompressType::Z_BEST_COMPRESS);
    std::string input("abc");
    EXPECT_CALL(*mock, write(_)).WillOnce(Return(WorkerState::EC_OK));
    ASSERT_EQ(WorkerState::EC_OK, state->write(input));

    EXPECT_CALL(*mock, write(_)).WillOnce(Return(WorkerState::EC_FAIL));
    ASSERT_EQ(WorkerState::EC_FAIL, state->write(input));

    EXPECT_CALL(*mock, write(_)).WillOnce(Return(WorkerState::EC_UPDATE));
    ASSERT_EQ(WorkerState::EC_UPDATE, state->write(input));
}

} // namespace worker_base
} // namespace worker_framework
