#include "suez/table/TableLeaderInfoPublisher.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <string>

#include "MockLeaderElectionManager.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class TableLeaderInfoPublisherTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TableLeaderInfoPublisherTest::setUp() {}

void TableLeaderInfoPublisherTest::tearDown() {}

class MockTableLeaderInfoPublisher : public TableLeaderInfoPublisher {
public:
    MockTableLeaderInfoPublisher(const PartitionId &pid,
                                 const std::string &zoneName,
                                 TableInfoPublishWrapper *publishWrapper,
                                 LeaderElectionManager *leaderElectionMgr)
        : TableLeaderInfoPublisher(pid, zoneName, publishWrapper, leaderElectionMgr) {}
    MOCK_METHOD0(publish, bool());
};

TEST_F(TableLeaderInfoPublisherTest, testUpdateLeaderInfo) {
    auto leaderMgr = std::make_unique<NiceMockLeaderElectionManager>();
    auto publisher =
        std::make_unique<MockTableLeaderInfoPublisher>(TableMetaUtil::makePid("t"), "z", nullptr, leaderMgr.get());

    EXPECT_CALL(*leaderMgr, getRoleType(_)).WillOnce(Return(RT_FOLLOWER));
    EXPECT_CALL(*publisher, publish()).WillOnce(Return(true));
    ASSERT_TRUE(publisher->updateLeaderInfo());
    ASSERT_TRUE(publisher->_leaderInfoPublished);

    EXPECT_CALL(*leaderMgr, getRoleType(_)).WillOnce(Return(RT_LEADER));
    EXPECT_CALL(*publisher, publish()).WillOnce(Return(false));
    ASSERT_FALSE(publisher->updateLeaderInfo());
    ASSERT_FALSE(publisher->_leaderInfoPublished);

    EXPECT_CALL(*leaderMgr, getRoleType(_)).WillOnce(Return(RT_LEADER));
    EXPECT_CALL(*publisher, publish()).WillOnce(Return(true));
    ASSERT_TRUE(publisher->updateLeaderInfo());
    ASSERT_TRUE(publisher->_leaderInfoPublished);

    EXPECT_CALL(*leaderMgr, getRoleType(_)).WillOnce(Return(RT_LEADER));
    EXPECT_CALL(*publisher, publish()).Times(0);
    ASSERT_TRUE(publisher->updateLeaderInfo());
    ASSERT_TRUE(publisher->_leaderInfoPublished);

    EXPECT_CALL(*leaderMgr, getRoleType(_)).WillOnce(Return(RT_FOLLOWER));
    EXPECT_CALL(*publisher, publish()).WillOnce(Return(false));
    ASSERT_FALSE(publisher->updateLeaderInfo());
    ASSERT_FALSE(publisher->_leaderInfoPublished);

    EXPECT_CALL(*leaderMgr, getRoleType(_)).WillOnce(Return(RT_FOLLOWER));
    EXPECT_CALL(*publisher, publish()).WillOnce(Return(true));
    ASSERT_TRUE(publisher->updateLeaderInfo());
    ASSERT_TRUE(publisher->_leaderInfoPublished);

    EXPECT_CALL(*leaderMgr, getRoleType(_)).WillOnce(Return(RT_FOLLOWER));
    EXPECT_CALL(*publisher, publish()).Times(0);
    ASSERT_TRUE(publisher->updateLeaderInfo());
    ASSERT_TRUE(publisher->_leaderInfoPublished);
}

} // namespace suez
