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
#include "fslib/util/RegularExpr.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(util);
AUTIL_DECLARE_AND_SETUP_LOGGER(util, RegularExpr);

RegularExpr::RegularExpr() : _init(false) {}

RegularExpr::~RegularExpr() {
    if (_init) {
        regfree(&_regex);
    }
}

bool RegularExpr::init(const string &pattern) {
    if (_init) {
        return false;
    }

    if (regcomp(&_regex, pattern.c_str(), REG_EXTENDED | REG_NOSUB) == 0) {
        _init = true;
        return true;
    }

    AUTIL_LOG(ERROR, "init fail with pattern [%s]", pattern.c_str());
    return false;
}

bool RegularExpr::match(const string &string) const {
    if (!_init) {
        return false;
    }
    return regexec(&_regex, string.c_str(), 0, NULL, 0) == 0;
}

FSLIB_END_NAMESPACE(util);
