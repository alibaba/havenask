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
#ifndef __INDEXLIB_TERM_HINT_PARSER_H
#define __INDEXLIB_TERM_HINT_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"

namespace indexlib { namespace common {

class TermHintParser
{
public:
    TermHintParser();
    ~TermHintParser();

public:
    static int32_t GetHintValues(const std::vector<std::string>& inputs);
    static std::vector<index::TemperatureProperty> GetHintProperty(int32_t hintValues);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TermHintParser);

}} // namespace indexlib::common

#endif //__INDEXLIB_TERM_HINT_PARSER_H
