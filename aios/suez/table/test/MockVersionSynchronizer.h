#pragma once

#include "suez/table/VersionSynchronizer.h"
#include "unittest/unittest.h"

namespace suez {

class MockVersionSynchronizer : public VersionSynchronizer {
public:
    MockVersionSynchronizer() {
        ON_CALL(*this, syncFromPersist(_, _, _)).WillByDefault(Return(true));
        ON_CALL(*this, persistVersion(_, _, _)).WillByDefault(Return(true));
        ON_CALL(*this, getVersionList(_, _)).WillByDefault(Return(true));
        ON_CALL(*this, updateVersionList(_, _)).WillByDefault(Return(true));
    }

public:
    bool supportSyncFromPersist(const PartitionMeta &target) const override { return true; }
    MOCK_METHOD0(init, bool());
    MOCK_METHOD1(remove, void(const PartitionId &));
    MOCK_METHOD3(syncFromPersist, bool(const PartitionId &, const std::string &, TableVersion &));
    MOCK_METHOD3(persistVersion, bool(const PartitionId &, const std::string &, const TableVersion &));
    MOCK_METHOD2(getVersionList, bool(const PartitionId &, std::vector<TableVersion> &));
    MOCK_METHOD2(updateVersionList, bool(const PartitionId &, const std::vector<TableVersion> &));
};

typedef ::testing::StrictMock<MockVersionSynchronizer> StrictMockVersionSynchronizer;
typedef ::testing::NiceMock<MockVersionSynchronizer> NiceMockVersionSynchronizer;

} // namespace suez
