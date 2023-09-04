#ifndef ISEARCH_BS_MOCKLEADERELECTOR_H
#define ISEARCH_BS_MOCKLEADERELECTOR_H
#include "gmock/gmock.h"

#include "test/test.h"
#include "unittest/unittest.h"
#include "worker_framework/LeaderElector.h"

namespace worker_framework {

class MockLeaderElector : public LeaderElector {
public:
    MockLeaderElector(cm_basic::ZkWrapper *zkWrapper,
                      const std::string &leaderPath,
                      int64_t leaseInSec,
                      int64_t loopInterval)
        : LeaderElector(zkWrapper, leaderPath, leaseInSec, loopInterval) {
        ON_CALL(*this, unlock()).WillByDefault(Invoke(std::bind(&MockLeaderElector::unlockFoo, this)));
        ON_CALL(*this, checkStillLeader(_))
            .WillByDefault(Invoke(std::bind(&MockLeaderElector::checkStillLeaderFoo, this, std::placeholders::_1)));
        ON_CALL(*this, reletLeaderTime(_))
            .WillByDefault(Invoke(std::bind(&MockLeaderElector::reletLeaderTimeFoo, this, std::placeholders::_1)));
        ON_CALL(*this, getLeaderLeaseExpirationTime(_, _))
            .WillByDefault(Invoke(std::bind(&MockLeaderElector::getLeaderLeaseExpirationTimeFoo,
                                            this,
                                            std::placeholders::_1,
                                            std::placeholders::_2)));
        ON_CALL(*this, lockForLease()).WillByDefault(Invoke(std::bind(&MockLeaderElector::lockForLeaseFoo, this)));
        ON_CALL(*this, unlockForLease()).WillByDefault(Invoke(std::bind(&MockLeaderElector::unlockForLeaseFoo, this)));
    }
    ~MockLeaderElector() {}

private:
    MOCK_METHOD0(unlock, void());
    MOCK_METHOD1(checkStillLeader, bool(int64_t));
    MOCK_METHOD1(reletLeaderTime, bool(int64_t));
    MOCK_METHOD2(getLeaderLeaseExpirationTime, bool(int64_t &, std::string &));
    MOCK_METHOD0(lockForLease, bool());
    MOCK_METHOD0(unlockForLease, bool());

private:
    void unlockFoo() { LeaderElector::unlock(); }
    bool checkStillLeaderFoo(int64_t currentTime) { return LeaderElector::checkStillLeader(currentTime); }
    bool reletLeaderTimeFoo(int64_t currentTime) { return LeaderElector::reletLeaderTime(currentTime); }
    bool getLeaderLeaseExpirationTimeFoo(int64_t &lastLeaderEndTime, std::string &progressKey) {
        return LeaderElector::getLeaderLeaseExpirationTime(lastLeaderEndTime, progressKey);
    }
    bool lockForLeaseFoo() { return LeaderElector::lockForLease(); }
    bool unlockForLeaseFoo() { return LeaderElector::unlockForLease(); }
};

typedef ::testing::StrictMock<MockLeaderElector> StrictMockLeaderElector;
typedef ::testing::NiceMock<MockLeaderElector> NiceMockLeaderElector;
}; // namespace worker_framework

#endif // ISEARCH_BS_MOCKLEADERELECTOR_H
