#include "indexlib/merger/split_strategy/test_split_strategy.h"

using namespace std;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TestSplitStrategy);

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::util;

const std::string TestSplitStrategy::STRATEGY_NAME = "test";

bool TestSplitStrategy::Init(const util::KeyValueMap& parameters)
{
    std::string segmentCountStr = GetValueFromKeyValueMap(parameters, "segment_count", "1");
    if (!(autil::StringUtil::fromString(segmentCountStr, mSegmentCount))) {
        IE_LOG(ERROR, "convert segment_count faile: [%s]", segmentCountStr.c_str());
        return false;
    }
    std::string useInvalidDocStr = GetValueFromKeyValueMap(parameters, "use_invalid_doc", "false");
    if (useInvalidDocStr == "true") {
        mUseInvaildDoc = true;
    }

    return true;
}
}} // namespace indexlib::merger
