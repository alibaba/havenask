#include "build_service/util/SwiftTopicStreamReaderCreator.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "build_service/test/unittest.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/testlib/MockSwiftClient.h"
#include "swift/testlib/MockSwiftReader.h"
#include "unittest/unittest.h"

namespace build_service::util {

class SwiftTopicStreamReaderCreatorTest : public BUILD_SERVICE_TESTBASE
{
};

TEST_F(SwiftTopicStreamReaderCreatorTest, testParse)
{
    std::vector<SwiftTopicStreamReaderCreator::TopicDesc> results;
    ASSERT_TRUE(SwiftTopicStreamReaderCreator::parseTopicList("", results));
    ASSERT_TRUE(results.empty());

    ASSERT_TRUE(SwiftTopicStreamReaderCreator::parseTopicList("t1:-1:-1", results));
    ASSERT_EQ(1, results.size());
    ASSERT_EQ("t1", results[0].topicName);
    ASSERT_EQ(0, results[0].startTimestamp);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), results[0].stopTimestamp);

    results.clear();
    ASSERT_TRUE(SwiftTopicStreamReaderCreator::parseTopicList("t1:4:88", results));
    ASSERT_EQ(1, results.size());
    ASSERT_EQ("t1", results[0].topicName);
    ASSERT_EQ(4, results[0].startTimestamp);
    ASSERT_EQ(88, results[0].stopTimestamp);

    results.clear();
    ASSERT_FALSE(SwiftTopicStreamReaderCreator::parseTopicList("t1:95:88", results));

    ASSERT_TRUE(SwiftTopicStreamReaderCreator::parseTopicList("t1:0:88#t2:127:-1", results));
    ASSERT_EQ(2, results.size());
    ASSERT_EQ("t1", results[0].topicName);
    ASSERT_EQ(0, results[0].startTimestamp);
    ASSERT_EQ(88, results[0].stopTimestamp);
    ASSERT_EQ("t2", results[1].topicName);
    ASSERT_EQ(127, results[1].startTimestamp);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), results[1].stopTimestamp);
}

TEST_F(SwiftTopicStreamReaderCreatorTest, testCreateReader)
{
    using NiceMockSwiftClient = ::testing::NiceMock<swift::testlib::MockSwiftClient>;
    auto client = std::make_unique<NiceMockSwiftClient>();

    {
        auto mockReader = std::make_unique<swift::testlib::NiceMockSwiftReader>();
        EXPECT_CALL(*mockReader, seekByTimestamp(_, _)).Times(0);
        EXPECT_CALL(*mockReader, setTimestampLimit(12345678, _)).Times(1);
        EXPECT_CALL(*client, createReader("topicName=t1", _)).WillOnce(Return(mockReader.release()));
        auto reader = SwiftTopicStreamReaderCreator::createReaderForTopic(client.get(), "", "t1", 0, 12345678, "");
        ASSERT_TRUE(reader);
    }

    {
        auto mockReader = std::make_unique<swift::testlib::NiceMockSwiftReader>();
        EXPECT_CALL(*mockReader, seekByTimestamp(_, _)).Times(0);
        EXPECT_CALL(*mockReader, setTimestampLimit(12345678, _)).Times(1);
        EXPECT_CALL(*client, createReader("zkPath=zk1;topicName=t1", _)).WillOnce(Return(mockReader.release()));
        auto reader = SwiftTopicStreamReaderCreator::createReaderForTopic(client.get(), "zk1", "t1", 0, 12345678, "");
        ASSERT_TRUE(reader);
    }

    {
        auto mockReader = std::make_unique<swift::testlib::NiceMockSwiftReader>();
        EXPECT_CALL(*mockReader, seekByTimestamp(100, _))
            .WillOnce(Return(swift::protocol::ERROR_CLIENT_INVALID_PARAMETERS));
        EXPECT_CALL(*mockReader, setTimestampLimit(12345678, _)).Times(0);
        EXPECT_CALL(*client, createReader("topicName=t1", _)).WillOnce(Return(mockReader.release()));
        auto reader = SwiftTopicStreamReaderCreator::createReaderForTopic(client.get(), "", "t1", 100, 12345678, "");
        ASSERT_FALSE(reader);
    }
}

} // namespace build_service::util
