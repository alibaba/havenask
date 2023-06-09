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
#ifndef __INDEXLIB_MATCH_FUNCTION_H
#define __INDEXLIB_MATCH_FUNCTION_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace index {

class MatchFunction
{
public:
    MatchFunction() {}
    virtual ~MatchFunction() {}

public:
    virtual bool Init(util::KeyValueMap param) = 0;
    virtual void Execute(int8_t* inputValue, int8_t* outputValue) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MatchFunction);
}} // namespace indexlib::index

#endif //__INDEXLIB_MATCH_FUNCTION_H
