#pragma once
#include "unittest/unittest.h"
#include "navi/log/NaviLoggerProvider.h"

namespace navi {

class NaviLoggerProviderTestUtil {
public:
    static void checkTraceCount(size_t expectedNums,
                                const std::string &filter,
                                navi::NaviLoggerProvider &provider);
};

}
