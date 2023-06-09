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
#include "indexlib/test/range_query_parser.h"

#include "autil/StringUtil.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;
namespace indexlib { namespace test {
IE_LOG_SETUP(test, RangeQueryParser);

RangeQueryParser::RangeQueryParser() {}

RangeQueryParser::~RangeQueryParser() {}

bool RangeQueryParser::ParseQueryStr(const config::IndexConfigPtr& indexConfig, const std::string& range,
                                     int64_t& leftTimestamp, int64_t& rightTimestamp)
{
    if (range.empty()) {
        return false;
    }

    std::vector<std::string> strVec = StringUtil::split(range, ",");
    if ((int)strVec.size() != 2 || strVec[0].length() < 1 || strVec[1].length() < 1) {
        return false;
    }

    if (strVec[0][0] != '(' && strVec[0][0] != '[') {
        return false;
    }

    if (strVec[1].back() != ')' && strVec[1].back() != ']') {
        return false;
    }
    int rightNumberLength = (int)strVec[1].length() - 1;
    bool isLeftClose = strVec[0][0] == '[';
    bool isRightClose = strVec[1].back() == ']';
    if (strVec[0].length() == 1 && strVec[0][0] == '(') {
        leftTimestamp = std::numeric_limits<int64_t>::min();
        isLeftClose = true;
    } else if (!ParseTimestamp(indexConfig, strVec[0].substr(1), leftTimestamp)) {
        return false;
    }
    if (strVec[1].length() == 1 && strVec[1][0] == ')') {
        rightTimestamp = std::numeric_limits<int64_t>::max();
        isRightClose = true;
    } else if (!ParseTimestamp(indexConfig, strVec[1].substr(0, rightNumberLength), rightTimestamp)) {
        return false;
    }
    if (!isLeftClose)
        leftTimestamp++;
    if (!isRightClose)
        rightTimestamp--;

    return leftTimestamp <= rightTimestamp;
}

bool RangeQueryParser::ParseTimestamp(const IndexConfigPtr& indexConfig, const string& str, int64_t& value)
{
    InvertedIndexType type = indexConfig->GetInvertedIndexType();
    if (it_range == type) {
        return StringUtil::numberFromString(str, value);
    }

    assert(it_datetime == indexConfig->GetInvertedIndexType());
    DateIndexConfigPtr dateIndexConfig = DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    FieldConfigPtr fieldConfig = dateIndexConfig->GetFieldConfig();
    uint64_t tmp;
    if (!TimestampUtil::ConvertToTimestamp(fieldConfig->GetFieldType(), StringView(str), tmp,
                                           fieldConfig->GetDefaultTimeZoneDelta()) &&
        !TimestampUtil::ConvertToTimestamp(ft_uint64, StringView(str), tmp, 0)) {
        return false;
    }
    value = tmp;
    return true;
}
}} // namespace indexlib::test
