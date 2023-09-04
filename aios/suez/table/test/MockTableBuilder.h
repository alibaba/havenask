#pragma once

#include "suez/table/TableBuilder.h"
#include "unittest/unittest.h"

namespace suez {

class MockTableBuilder : public TableBuilder {
public:
    MOCK_METHOD0(start, bool());
    MOCK_METHOD0(stop, void());
    MOCK_METHOD0(suspend, void());
    MOCK_METHOD0(resume, void());
    MOCK_METHOD1(skip, void(int64_t));
    MOCK_METHOD0(forceRecover, void());
    MOCK_METHOD0(isRecovered, bool());
    MOCK_CONST_METHOD0(needCommit, bool());
    MOCK_METHOD0(commit, std::pair<bool, TableVersion>());
};
using NiceMockTableBuilder = ::testing::NiceMock<MockTableBuilder>;

} // namespace suez
