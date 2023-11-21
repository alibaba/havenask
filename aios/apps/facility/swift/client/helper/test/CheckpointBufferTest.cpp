#include "swift/client/helper/CheckpointBuffer.h"

#include <algorithm>
#include <cstdint>
#include <gmock/gmock-matchers.h>
#include <iosfwd>
#include <vector>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::client;

namespace swift {
namespace client {

class CheckpointBufferTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void CheckpointBufferTest::setUp() {}

void CheckpointBufferTest::tearDown() {}

TEST_F(CheckpointBufferTest, testGetOffset) {
    CheckpointBuffer buffer;
    buffer._startOffset = CheckpointBuffer::BUF_SIZE / 2;
    buffer._startId = 10;
    ASSERT_EQ(buffer._startOffset + 2, buffer.getOffset(buffer._startId + 2));
    ASSERT_EQ(buffer._startOffset + 0, buffer.getOffset(buffer._startId + 0));
    ASSERT_EQ(buffer._startOffset - 1, buffer.getOffset(buffer._startId + CheckpointBuffer::BUF_SIZE - 1));
}

TEST_F(CheckpointBufferTest, testUpdateCommittedId) {
    CheckpointBuffer buffer;
    {
        buffer.init(100);
        ASSERT_FALSE(buffer.updateCommittedId());
        ASSERT_EQ(99, buffer._committedId);
    }
    {
        buffer.init(100);
        buffer._buffer[1] = 1;
        ASSERT_FALSE(buffer.updateCommittedId());
        ASSERT_EQ(99, buffer._committedId);
    }
    {
        buffer.init(100);
        buffer._buffer[0] = 1;
        ASSERT_TRUE(buffer.updateCommittedId());
        ASSERT_EQ(100, buffer._committedId);
    }
    {
        buffer.init(100);
        buffer._buffer[0] = 1;
        buffer._buffer[2] = 2;
        ASSERT_TRUE(buffer.updateCommittedId());
        ASSERT_EQ(100, buffer._committedId);
    }
    {
        buffer.init(100);
        buffer._buffer[0] = 1;
        buffer._buffer[1] = 2;
        ASSERT_TRUE(buffer.updateCommittedId());
        ASSERT_EQ(101, buffer._committedId);
    }
    {
        buffer.init(100);
        buffer._buffer.clear();
        buffer._buffer.resize(CheckpointBuffer::BUF_SIZE, 1);
        buffer._startOffset = 3;
        buffer._buffer[0] = CheckpointBuffer::INVALID_TS;
        ASSERT_TRUE(buffer.updateCommittedId());
        ASSERT_EQ(CheckpointBuffer::BUF_SIZE - 1 - 3 + 100, buffer._committedId);
    }
    {
        buffer.init(100);
        buffer._buffer.clear();
        buffer._buffer.resize(CheckpointBuffer::BUF_SIZE, 1);
        buffer._startOffset = 3;
        buffer._buffer[1] = CheckpointBuffer::INVALID_TS;
        ASSERT_TRUE(buffer.updateCommittedId());
        ASSERT_EQ(CheckpointBuffer::BUF_SIZE - 3 + 100, buffer._committedId);
    }
}

TEST_F(CheckpointBufferTest, testStealRange) {
    std::vector<int64_t> timestamps;
    CheckpointBuffer buffer;
    buffer.init(100);
    vector<int64_t> data = {1, 2, 3};
    std::copy(data.begin(), data.end(), buffer._buffer.begin());
    ASSERT_TRUE(buffer.updateCommittedId());

    buffer.stealRange(timestamps, 99);
    ASSERT_THAT(timestamps, ElementsAre());
    ASSERT_EQ(0, buffer._startOffset);
    ASSERT_EQ(100, buffer._startId);
    ASSERT_EQ(1, buffer._buffer[0]);
    ASSERT_EQ(2, buffer._buffer[1]);
    ASSERT_EQ(3, buffer._buffer[2]);

    buffer.stealRange(timestamps, 101);
    ASSERT_THAT(timestamps, ElementsAre(1, 2));
    ASSERT_EQ(2, buffer._startOffset);
    ASSERT_EQ(102, buffer._startId);
    ASSERT_EQ(CheckpointBuffer::INVALID_TS, buffer._buffer[0]);
    ASSERT_EQ(CheckpointBuffer::INVALID_TS, buffer._buffer[1]);
    ASSERT_EQ(3, buffer._buffer[2]);

    buffer.stealRange(timestamps, 102);
    ASSERT_THAT(timestamps, ElementsAre(3));
    ASSERT_EQ(3, buffer._startOffset);
    ASSERT_EQ(103, buffer._startId);
    ASSERT_EQ(CheckpointBuffer::INVALID_TS, buffer._buffer[0]);
    ASSERT_EQ(CheckpointBuffer::INVALID_TS, buffer._buffer[1]);
    ASSERT_EQ(CheckpointBuffer::INVALID_TS, buffer._buffer[2]);
}

TEST_F(CheckpointBufferTest, testStealRange_Ring) {
    std::vector<int64_t> timestamps;
    CheckpointBuffer buffer;
    buffer.init(100);
    buffer._startOffset = CheckpointBuffer::BUF_SIZE - 1;
    buffer._buffer[CheckpointBuffer::BUF_SIZE - 1] = 1;
    buffer._buffer[0] = 2;
    ASSERT_TRUE(buffer.updateCommittedId());

    buffer.stealRange(timestamps, 101);
    ASSERT_THAT(timestamps, ElementsAre(1, 2));
    ASSERT_EQ(1, buffer._startOffset);
    ASSERT_EQ(102, buffer._startId);
}

} // namespace client
} // namespace swift
