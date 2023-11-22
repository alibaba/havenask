#pragma once

#include "suez/drc/LogRewriter.h"
#include "unittest/unittest.h"

namespace suez {

class MockLogRewriter : public LogRewriter {
public:
    MOCK_METHOD1(init, bool(indexlibv2::framework::ITablet *index));
    MOCK_METHOD1(rewrite, RewriteCode(LogRecord &));
    MOCK_METHOD0(createSnapshot, bool());
    MOCK_METHOD0(releaseSnapshot, void());
};

typedef ::testing::StrictMock<MockLogRewriter> StrictMockLogRewriter;
typedef ::testing::NiceMock<MockLogRewriter> NiceMockLogRewriter;

} // namespace suez
