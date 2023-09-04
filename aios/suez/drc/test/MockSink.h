#pragma once

#include "suez/drc/Sink.h"
#include "unittest/unittest.h"

namespace suez {

class MockSink : public Sink {
public:
    MOCK_CONST_METHOD0(getType, const std::string &());
    MOCK_CONST_METHOD0(getCommittedCheckpoint, int64_t());
    MOCK_METHOD3(write, bool(uint32_t, std::string, int64_t));
};

typedef ::testing::StrictMock<MockSink> StrictMockSink;
typedef ::testing::NiceMock<MockSink> NiceMockSink;

} // namespace suez
