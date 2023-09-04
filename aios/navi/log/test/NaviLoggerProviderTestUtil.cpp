#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "autil/StringUtil.h"

namespace navi {

void NaviLoggerProviderTestUtil::checkTraceCount(
        size_t expectedNums,
        const std::string &filter,
        navi::NaviLoggerProvider &provider) {
    auto traces = provider.getTrace("");
    size_t count = std::count_if(traces.begin(), traces.end(),
                                 [&](auto &text) {
                                     return text.find(filter) != std::string::npos;
                                 });
    ASSERT_EQ(expectedNums, count) << "filter: " << filter << std::endl
                                   << "traces: " << autil::StringUtil::toString(traces);
}

}
