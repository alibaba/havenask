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
#ifndef __INDEXLIB_Range_QUERY_PARSER_H
#define __INDEXLIB_Range_QUERY_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace test {

class RangeQueryParser
{
public:
    RangeQueryParser();
    ~RangeQueryParser();

public:
    static bool ParseQueryStr(const config::IndexConfigPtr& indexConfig, const std::string& range,
                              int64_t& leftTimestamp, int64_t& rightTimestamp);

    static bool ParseTimestamp(const config::IndexConfigPtr& indexConfig, const std::string& str, int64_t& value);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeQueryParser);
}} // namespace indexlib::test

#endif //__INDEXLIB_Range_QUERY_PARSER_H
