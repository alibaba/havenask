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
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"

using namespace std;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, DateLevelFormat);

DateLevelFormat::~DateLevelFormat() {}

void DateLevelFormat::Init(Granularity granularity)
{
    _levelFormat = DEFALT_LEVEL_FORMAT;
    RemoveLowerGranularity(granularity);
}

void DateLevelFormat::RemoveLowerGranularity(Granularity granularity)
{
    if (granularity == YEAR) {
        _levelFormat &= ~((1 << 11) - 1);
        return;
    }

    if (granularity == MONTH) {
        _levelFormat &= ~((1 << 10) - 1);
        return;
    }

    if (granularity == DAY) {
        _levelFormat &= ~((1 << 8) - 1);
        return;
    }

    if (granularity == HOUR) {
        _levelFormat &= ~((1 << 6) - 1);
        return;
    }

    if (granularity == MINUTE) {
        _levelFormat &= ~((1 << 4) - 1);
        return;
    }

    if (granularity == SECOND) {
        _levelFormat &= ~((1 << 2) - 1);
        return;
    }
}

uint64_t DateLevelFormat::GetGranularityLevel(Granularity granularity)
{
    switch (granularity) {
    case MILLISECOND:
        return 1;
    case SECOND:
        return 3;
    case MINUTE:
        return 5;
    case HOUR:
        return 7;
    case DAY:
        return 9;
    case MONTH:
        return 11;
    case YEAR:
        return 12;
    default:
        assert(false);
    }
    return 0;
}
} // namespace indexlib::config
