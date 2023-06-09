/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/config/RankSizeConverter.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, RankSizeConverter);

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
        value = std::numeric_limits<uint32_t>::max();
    } else if (strRankSize.empty()) {
        value = 0;
    } else {
        if (!autil::StringUtil::strToUInt32(strRankSize.c_str(), value) 
            || strRankSize != autil::StringUtil::toString(value) ) 
        {
            value = 0;
            AUTIL_LOG(WARN, "Wrong parameter for rank size:%s", rankSize.c_str());
        }
    }
    return value;
}

} // namespace config
} // namespace isearch

