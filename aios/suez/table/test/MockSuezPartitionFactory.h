#pragma once

#include "suez/table/SuezPartitionFactory.h"
#include "unittest/unittest.h"

namespace suez {

class MockSuezPartitionFactory : public SuezPartitionFactory {
public:
    MOCK_METHOD3(create, SuezPartitionPtr(SuezPartitionType, const TableResource &, const CurrentPartitionMetaPtr &));
};
typedef ::testing::NiceMock<MockSuezPartitionFactory> NiceMockSuezPartitionFactory;
typedef ::testing::StrictMock<MockSuezPartitionFactory> StrictMockSuezPartitionFactory;

} // namespace suez
