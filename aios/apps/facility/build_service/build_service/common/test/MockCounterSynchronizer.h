#pragma once

#include "build_service/common/CounterSynchronizer.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class MockCounterSynchronizer : public CounterSynchronizer
{
public:
    MockCounterSynchronizer() {}
    ~MockCounterSynchronizer() {}

public:
    MOCK_CONST_METHOD0(sync, bool());
};

typedef ::testing::StrictMock<MockCounterSynchronizer> StrictMockCounterSynchronizer;
typedef ::testing::NiceMock<MockCounterSynchronizer> NiceMockCounterSynchronizer;

}} // namespace build_service::common
