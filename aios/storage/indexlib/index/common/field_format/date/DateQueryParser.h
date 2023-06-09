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
#pragma once
#include <memory>

#include "indexlib/index/common/Term.h"
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"

namespace indexlib::index {

class DateQueryParser
{
public:
    DateQueryParser();
    ~DateQueryParser();

public:
    void Init(const config::DateLevelFormat::Granularity& buildGranularity, const config::DateLevelFormat& format);

    bool ParseQuery(const index::Term* term, uint64_t& leftTerm, uint64_t& rightTerm);

private:
    uint64_t _searchGranularityUnit;
    config::DateLevelFormat _format;

private:
    friend class DateQueryParserTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
