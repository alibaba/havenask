#pragma once

#include "suez/table/LeaderElectionManager.h"
#include "unittest/unittest.h"

namespace suez {

class MockLeaderElectionManager : public LeaderElectionManager {
public:
    MockLeaderElectionManager() : LeaderElectionManager(LeaderElectionConfig()) {}
    MOCK_METHOD0(init, bool());
    MOCK_METHOD0(stop, void());
    MOCK_METHOD2(registerPartition, bool(const PartitionId &, LeaderChangeHandler));
    MOCK_METHOD1(removePartition, void(const PartitionId &));
    MOCK_METHOD1(releaseLeader, void(const PartitionId &));
    MOCK_METHOD1(seal, void(const PartitionId &));
    MOCK_METHOD1(allowCampaginLeader, void(const PartitionId &));
    MOCK_CONST_METHOD1(getRoleType, RoleType(const PartitionId &));
    MOCK_METHOD1(getLeaderTimestamp, int64_t(const PartitionId &));
    MOCK_METHOD1(getLeaderElectionInfo, std::pair<bool, int64_t>(const PartitionId &));
    MOCK_METHOD2(setCampaginLeaderIndication, void(const PartitionId &, bool));
};

typedef ::testing::StrictMock<MockLeaderElectionManager> StrictMockLeaderElectionManager;
typedef ::testing::NiceMock<MockLeaderElectionManager> NiceMockLeaderElectionManager;

} // namespace suez
