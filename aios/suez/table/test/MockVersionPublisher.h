#pragma once

#include "suez/table/VersionPublisher.h"
#include "unittest/unittest.h"

namespace suez {

class MockVersionPublisher : public VersionPublisher {
public:
    MOCK_METHOD1(remove, void(const PartitionId &));
    MOCK_METHOD3(publish, bool(const std::string &, const PartitionId &, const TableVersion &));
    MOCK_METHOD4(getPublishedVersion, bool(const std::string &, const PartitionId &pid, IncVersion, TableVersion &));
};

typedef ::testing::StrictMock<MockVersionPublisher> StrictMockVersionPublisher;
typedef ::testing::NiceMock<MockVersionPublisher> NiceMockVersionPublisher;

} // namespace suez
