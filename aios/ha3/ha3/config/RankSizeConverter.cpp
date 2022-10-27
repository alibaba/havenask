#include <ha3/config/RankSizeConverter.h>
#include <autil/StringUtil.h>
#include <ha3/util/NumericLimits.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, RankSizeConverter);

RankSizeConverter::RankSizeConverter() { 
}

RankSizeConverter::~RankSizeConverter() { 
}

uint32_t RankSizeConverter::convertToRankSize(const string &rankSize){
    string strRankSize;
    uint32_t value = 0;
    
    StringUtil::toUpperCase(rankSize.c_str(), strRankSize);
    StringUtil::trim(strRankSize);

    if (strRankSize == "UNLIMITED") {
        value = util::NumericLimits<uint32_t>::max();
    } else if (strRankSize.empty()) {
        value = 0;
    } else {
        if (!autil::StringUtil::strToUInt32(strRankSize.c_str(), value) 
            || strRankSize != autil::StringUtil::toString(value) ) 
        {
            value = 0;
            HA3_LOG(WARN, "Wrong parameter for rank size:%s", rankSize.c_str());
        }
    }
    return value;
}

END_HA3_NAMESPACE(config);

