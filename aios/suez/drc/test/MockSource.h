#pragma once

#include "suez/drc/Source.h"
#include "unittest/unittest.h"

namespace suez {

class MockSource : public Source {
public:
    MOCK_CONST_METHOD0(getType, const std::string &());
    MOCK_METHOD1(seek, bool(int64_t));
    MOCK_METHOD2(read, bool(std::string &, int64_t &));
    MOCK_CONST_METHOD0(getLatestLogId, int64_t());
};

typedef ::testing::StrictMock<MockSource> StrictMockSource;
typedef ::testing::NiceMock<MockSource> NiceMockSource;

} // namespace suez
