#pragma once
#include "sql/ops/tvf/TvfFunc.h"
#include "unittest/unittest.h"

namespace sql {

class MockTvfFunc : public TvfFunc {
public:
    MOCK_METHOD1(init, bool(TvfFuncInitContext &));
    MOCK_METHOD3(compute, bool(const table::TablePtr &, bool, table::TablePtr &));
};

} // namespace sql
