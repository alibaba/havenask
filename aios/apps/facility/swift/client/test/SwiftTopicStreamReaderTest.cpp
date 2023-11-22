#include "swift/client/SwiftTopicStreamReader.h"

#include <cstdint>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-nice-strict.h>
#include <gtest/gtest-matchers.h>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "MockSwiftReader.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace ::testing;

namespace swift {
namespace client {

class SwiftTopicStreamReaderTest : public TESTBASE {
public:
    void setUp() override {
        {
            auto reader = std::make_unique<NiceMockSwiftReader>();
            _mockReaders.push_back(reader.get());

            SingleTopicReader topicReader{std::move(reader), "t1", 0, 12345};
            _readers.emplace_back(std::move(topicReader));
        }
        {
            auto reader = std::make_unique<NiceMockSwiftReader>();
            _mockReaders.push_back(reader.get());
            SingleTopicReader topicReader{std::move(reader), "t2", 12340, std::numeric_limits<int64_t>::max()};
            _readers.emplace_back(std::move(topicReader));
        }
    }

private:
    std::vector<SingleTopicReader> _readers;
    std::vector<MockSwiftReader *> _mockReaders;
};

TEST_F(SwiftTopicStreamReaderTest, testInit) {
    {
        SwiftTopicStreamReader reader;
        EXPECT_EQ(-1, reader._currentIdx);
        EXPECT_EQ(nullptr, reader._currentReader);

        EXPECT_EQ(protocol::ERROR_CLIENT_INIT_INVALID_PARAMETERS, reader.init(std::vector<SingleTopicReader>{}));
    }
    {
        SwiftTopicStreamReader reader;
        EXPECT_EQ(protocol::ERROR_NONE, reader.init(std::move(_readers)));
    }
}

TEST_F(SwiftTopicStreamReaderTest, testSeekByTimestamp) {
    SwiftTopicStreamReader reader;
    ASSERT_EQ(protocol::ERROR_NONE, reader.init(std::move(_readers)));

    EXPECT_EQ(protocol::ERROR_CLIENT_INVALID_PARAMETERS, reader.seekByTimestamp(-1, false));
    EXPECT_EQ(protocol::ERROR_CLIENT_INVALID_PARAMETERS,
              reader.seekByTimestamp(std::numeric_limits<int64_t>::max(), false));

    EXPECT_CALL(*_mockReaders[0], seekByTimestamp(_, _)).WillOnce(Return(protocol::ERROR_NONE));
    EXPECT_EQ(protocol::ERROR_NONE, reader.seekByTimestamp(1, false));
    EXPECT_EQ(0, reader._currentIdx);

    EXPECT_CALL(*_mockReaders[0], seekByTimestamp(_, _)).WillOnce(Return(protocol::ERROR_NONE));
    EXPECT_EQ(protocol::ERROR_NONE, reader.seekByTimestamp(12341, false));
    EXPECT_EQ(0, reader._currentIdx);

    EXPECT_CALL(*_mockReaders[1], seekByTimestamp(_, _)).WillOnce(Return(protocol::ERROR_NONE));
    EXPECT_EQ(protocol::ERROR_NONE, reader.seekByTimestamp(12345, false));
    EXPECT_EQ(1, reader._currentIdx);

    EXPECT_CALL(*_mockReaders[1], seekByTimestamp(_, _)).WillOnce(Return(protocol::ERROR_NONE));
    EXPECT_EQ(protocol::ERROR_NONE, reader.seekByTimestamp(22345, false));
    EXPECT_EQ(1, reader._currentIdx);
}

TEST_F(SwiftTopicStreamReaderTest, testSeekByProgress) {
    SwiftTopicStreamReader reader;
    ASSERT_EQ(protocol::ERROR_NONE, reader.init(std::move(_readers)));

    protocol::ReaderProgress progress;
    ASSERT_EQ(protocol::ERROR_CLIENT_INVALID_PARAMETERS, reader.seekByProgress(progress, false));

    auto *p = progress.add_topicprogress();
    p->set_topicname("t_xx");
    p->add_partprogress()->set_timestamp(2);
    ASSERT_EQ(protocol::ERROR_CLIENT_INVALID_PARAMETERS, reader.seekByProgress(progress, false));

    p->set_topicname("t1");
    EXPECT_CALL(*_mockReaders[0], seekByProgress(_, _)).WillOnce(Return(protocol::ERROR_NONE));
    ASSERT_EQ(protocol::ERROR_NONE, reader.seekByProgress(progress, false));
    ASSERT_EQ(0, reader._currentIdx);

    p->set_topicname("t2");
    EXPECT_CALL(*_mockReaders[1], seekByProgress(_, _)).WillOnce(Return(protocol::ERROR_NONE));
    ASSERT_EQ(protocol::ERROR_NONE, reader.seekByProgress(progress, false));
    ASSERT_EQ(1, reader._currentIdx);

    EXPECT_CALL(*_mockReaders[1], seekByProgress(_, _)).WillOnce(Return(protocol::ERROR_CLIENT_NO_MORE_MESSAGE));
    ASSERT_EQ(protocol::ERROR_CLIENT_NO_MORE_MESSAGE, reader.seekByProgress(progress, false));
}

TEST_F(SwiftTopicStreamReaderTest, testRead) {
    SwiftTopicStreamReader reader;
    ASSERT_EQ(protocol::ERROR_NONE, reader.init(std::move(_readers)));

    protocol::Message msg;
    EXPECT_CALL(*_mockReaders[0], read(Matcher<protocol::Message &>(_), Matcher<int64_t>(_)))
        .WillOnce(Return(protocol::ERROR_NONE));
    ASSERT_EQ(protocol::ERROR_NONE, reader.read(msg, 3));

    EXPECT_CALL(*_mockReaders[0], read(Matcher<protocol::Message &>(_), Matcher<int64_t>(_)))
        .WillOnce(Return(protocol::ERROR_BROKER_BUSY));
    ASSERT_EQ(protocol::ERROR_BROKER_BUSY, reader.read(msg, 3));

    EXPECT_CALL(*_mockReaders[0], read(Matcher<protocol::Message &>(_), Matcher<int64_t>(_)))
        .WillOnce(Return(protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT));
    EXPECT_CALL(*_mockReaders[1], read(Matcher<protocol::Message &>(_), Matcher<int64_t>(_)))
        .WillOnce(Return(protocol::ERROR_NONE));
    ASSERT_EQ(protocol::ERROR_NONE, reader.read(msg, 3));
    ASSERT_EQ(1, reader._currentIdx);

    EXPECT_CALL(*_mockReaders[1], read(Matcher<protocol::Message &>(_), Matcher<int64_t>(_)))
        .WillOnce(Return(protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT));
    ASSERT_EQ(protocol::ERROR_CLIENT_EOF, reader.read(msg, 3));
    ASSERT_EQ(2, reader._currentIdx);
    ASSERT_TRUE(nullptr == reader._currentReader);
}

} // namespace client
} // namespace swift
