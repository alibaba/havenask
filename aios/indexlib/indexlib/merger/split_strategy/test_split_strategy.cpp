#include "indexlib/merger/split_strategy/test_split_strategy.h"

using namespace std;

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, TestSplitStrategy);

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

const std::string TestSplitStrategy::STRATEGY_NAME = "test";

bool TestSplitStrategy::Init(const util::KeyValueMap& parameters) 
{
    std::string segmentCountStr = GetValueFromKeyValueMap(parameters, "segment_count", "1");
    if (!(autil::StringUtil::fromString(segmentCountStr, mSegmentCount)))
    {
        IE_LOG(ERROR, "convert segment_count faile: [%s]", segmentCountStr.c_str());
        return false;
    }
    std::string useInvalidDocStr = GetValueFromKeyValueMap(parameters, "use_invalid_doc", "false");
    if (useInvalidDocStr == "true") {
        mUseInvaildDoc = true;
    }

    return true;
}



IE_NAMESPACE_END(merger);

