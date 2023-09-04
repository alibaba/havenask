#include "suez/common/test/TableMetaUtil.h"
#include "suez/table/ZkLeaderElectionManager.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class LeaderElectorWrapperTest : public TESTBASE {};

using LeaderElector = worker_framework::LeaderElector;

class MockLeaderElector : public LeaderElector {
public:
    MockLeaderElector() : LeaderElector(nullptr, "") {}

public:
    MOCK_METHOD0(start, bool());
    MOCK_METHOD0(stop, void());
    MOCK_CONST_METHOD0(isLeader, bool());
    MOCK_CONST_METHOD0(getLeaseExpirationTime, int64_t());
    MOCK_METHOD0(tryLock, bool());
    MOCK_METHOD0(unlock, void());
};

TEST_F(LeaderElectorWrapperTest, testReleaseLeader) {
    auto impl = std::make_unique<MockLeaderElector>();
    MockLeaderElector *mock = impl.get();
    LeaderElectorWrapper wrapper(nullptr, std::move(impl));

    ASSERT_FALSE(wrapper.forceFollower);
    ASSERT_EQ(-1, wrapper.forceFollowerExpireTime);

    EXPECT_CALL(*mock, getLeaseExpirationTime()).WillOnce(Return(0));
    EXPECT_CALL(*mock, stop()).Times(1);
    wrapper.releaseLeader();
    ASSERT_TRUE(wrapper.forceFollower);
    ASSERT_TRUE(wrapper.forceFollowerExpireTime > 0);
    EXPECT_CALL(*mock, isLeader()).Times(0);
    ASSERT_FALSE(wrapper.isLeader());
}

TEST_F(LeaderElectorWrapperTest, testIsLeader) {
    auto impl = std::make_unique<MockLeaderElector>();
    MockLeaderElector *mock = impl.get();
    LeaderElectorWrapper wrapper(nullptr, std::move(impl));
    EXPECT_CALL(*mock, isLeader()).WillOnce(Return(true));
    EXPECT_TRUE(wrapper.isLeader());

    wrapper.forceFollowerTimeInMs = 2000;
    EXPECT_CALL(*mock, stop()).Times(1);
    wrapper.releaseLeader();
    EXPECT_CALL(*mock, isLeader()).Times(0);
    ASSERT_FALSE(wrapper.isLeader());
    sleep(3);
    EXPECT_CALL(*mock, isLeader()).Times(0);
    EXPECT_CALL(*mock, start()).WillOnce(Return(true));
    ASSERT_FALSE(wrapper.isLeader());
    ASSERT_FALSE(wrapper.forceFollower);
    ASSERT_EQ(-1, wrapper.forceFollowerExpireTime);
    EXPECT_CALL(*mock, isLeader()).WillOnce(Return(true));
    ASSERT_TRUE(wrapper.isLeader());
}

TEST_F(LeaderElectorWrapperTest, testLeaderChange) {
    auto impl = std::make_unique<MockLeaderElector>();
    LeaderElectorWrapper wrapper(nullptr, std::move(impl));

    auto pid1 = TableMetaUtil::makePid("t1", 0, 1, 2);
    auto pid2 = TableMetaUtil::makePid("t2", 0, 1, 2);

    size_t leaderCount = 0;
    size_t followerCount = 0;
    auto fn = [&](const PartitionId &pid, bool isLeader) mutable {
        if (isLeader) {
            ++leaderCount;
        } else {
            ++followerCount;
        }
    };
    wrapper.registerPartition(pid1, fn);
    wrapper.registerPartition(pid2, fn);
    ASSERT_EQ(2, wrapper.getRefCount());
    wrapper.becomeLeader();
    ASSERT_EQ(2, leaderCount);
    ASSERT_EQ(0, followerCount);
    wrapper.nolongerLeader();
    ASSERT_EQ(2, leaderCount);
    ASSERT_EQ(2, followerCount);
    leaderCount = followerCount = 0;
    wrapper.removePartition(pid1);
    wrapper.becomeLeader();
    ASSERT_EQ(1, leaderCount);
    wrapper.nolongerLeader();
    ASSERT_EQ(1, followerCount);
}

} // namespace suez
