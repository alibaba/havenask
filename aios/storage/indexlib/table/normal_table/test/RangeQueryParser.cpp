#include "indexlib/table/normal_table/test/RangeQueryParser.h"

#include "autil/StringUtil.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, RangeQueryParser);

RangeQueryParser::RangeQueryParser() {}

RangeQueryParser::~RangeQueryParser() {}

bool RangeQueryParser::ParseQueryStr(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedConfig,
                                     const std::string& range, int64_t& leftTimestamp, int64_t& rightTimestamp)
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
    } else if (!ParseTimestamp(invertedConfig, strVec[0].substr(1), leftTimestamp)) {
        return false;
    }
    if (strVec[1].length() == 1 && strVec[1][0] == ')') {
        rightTimestamp = std::numeric_limits<int64_t>::max();
        isRightClose = true;
    } else if (!ParseTimestamp(invertedConfig, strVec[1].substr(0, rightNumberLength), rightTimestamp)) {
        return false;
    }
    if (!isLeftClose)
        leftTimestamp++;
    if (!isRightClose)
        rightTimestamp--;

    return leftTimestamp <= rightTimestamp;
}

bool RangeQueryParser::ParseTimestamp(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedConfig,
                                      const std::string& str, int64_t& value)
{
    InvertedIndexType type = invertedConfig->GetInvertedIndexType();
    if (it_range == type) {
        return StringUtil::numberFromString(str, value);
    }

    assert(it_datetime == invertedConfig->GetInvertedIndexType());
    auto dateIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(invertedConfig);
    assert(dateIndexConfig);
    auto fieldConfig = dateIndexConfig->GetFieldConfig();
    uint64_t tmp;
    if (!TimestampUtil::ConvertToTimestamp(fieldConfig->GetFieldType(), StringView(str), tmp,
                                           fieldConfig->GetDefaultTimeZoneDelta()) &&
        !TimestampUtil::ConvertToTimestamp(ft_uint64, StringView(str), tmp, 0)) {
        return false;
    }
    value = tmp;
    return true;
}

} // namespace indexlibv2::table
