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
#include "indexlib/index/common/field_format/date/DateQueryParser.h"

#include "autil/CommonMacros.h"
#include "indexlib/index/common/NumberTerm.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DateQueryParser);

DateQueryParser::DateQueryParser() : _searchGranularityUnit(0) {}

DateQueryParser::~DateQueryParser() {}

void DateQueryParser::Init(const config::DateLevelFormat::Granularity& buildGranularity,
                           const config::DateLevelFormat& format)
{
    _searchGranularityUnit = DateTerm::GetSearchGranularityUnit(buildGranularity);
    _format = format;
}

bool DateQueryParser::ParseQuery(const Term* term, uint64_t& leftTerm, uint64_t& rightTerm)
{
    Int64Term* rangeTerm = (Int64Term*)term;

    bool leftIsZero = false;
    int64_t leftTimestamp = rangeTerm->GetLeftNumber();
    int64_t rightTimestamp = rangeTerm->GetRightNumber();
    if (unlikely(leftTimestamp <= 0)) {
        leftTimestamp = 0;
        leftIsZero = true;
    }

    if (unlikely(rightTimestamp < 0 || leftTimestamp > rightTimestamp)) {
        return false;
    }

    if (!leftIsZero) {
        leftTimestamp--;
    }
    DateTerm left = DateTerm::ConvertToDateTerm(util::TimestampUtil::ConvertToTM((uint64_t)leftTimestamp), _format);
    DateTerm right = DateTerm::ConvertToDateTerm(util::TimestampUtil::ConvertToTM((uint64_t)rightTimestamp), _format);

    if (!leftIsZero) {
        left.PlusUnit(_searchGranularityUnit);
    }
    leftTerm = left.GetKey();
    rightTerm = right.GetKey();
    return true;
}
} // namespace indexlib::index
