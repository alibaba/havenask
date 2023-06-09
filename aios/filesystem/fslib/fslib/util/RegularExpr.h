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
#ifndef FSLIB_REGULAREXPR_H
#define FSLIB_REGULAREXPR_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include <sys/types.h>
#include <regex.h>

FSLIB_BEGIN_NAMESPACE(util);

class RegularExpr
{
public:
    RegularExpr();
    ~RegularExpr();
    
public:
    bool init(const std::string &pattern);
    bool match(const std::string &string) const;

private:
    regex_t _regex;
    bool _init;
};

FSLIB_TYPEDEF_SHARED_PTR(RegularExpr);
typedef std::vector<RegularExprPtr> RegularExprVector;

FSLIB_END_NAMESPACE(util);

#endif //FSLIB_REGULAREXPR_H
