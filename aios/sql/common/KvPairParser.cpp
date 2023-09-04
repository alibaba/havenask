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
#include "sql/common/KvPairParser.h"

#include <assert.h>
#include <cstddef>
#include <utility>

#include "autil/StringUtil.h"

using namespace std;

namespace sql {
AUTIL_LOG_SETUP(sql, KvPairParser);

string KvPairParser::getNextTerm(const string &inputStr, char sep, size_t &start) {
    string ret;
    ret.reserve(64);
    while (start < inputStr.length()) {
        char c = inputStr[start];
        ++start;
        if (c == sep) {
            break;
        } else if (c == '\\') {
            if (start < inputStr.length()) {
                ret += inputStr[start];
                ++start;
            }
            continue;
        }
        ret += c;
    }
    autil::StringUtil::trim(ret);
    return ret;
}

void KvPairParser::parse(const string &originalStr,
                         char kvPairSplit,
                         char kvSplit,
                         map<string, string> &kvPair) {
    SetMap setMap
        = [&kvPair](const string &key, const string &value) { kvPair[key] = std::move(value); };
    parse(originalStr, kvPairSplit, kvSplit, setMap);
}

void KvPairParser::parse(const string &originalStr,
                         char kvPairSplit,
                         char kvSplit,
                         unordered_map<string, string> &kvPair) {
    SetMap setMap
        = [&kvPair](const string &key, const string &value) { kvPair[key] = std::move(value); };
    parse(originalStr, kvPairSplit, kvSplit, setMap);
}

void KvPairParser::parse(const string &originalStr, char kvPairSplit, char kvSplit, SetMap setMap) {
    size_t start = 0;
    while (start < originalStr.length()) {
        string key = getNextTerm(originalStr, kvSplit, start);
        assert(start > 0);
        if (start >= originalStr.length()) {
            if (!key.empty()) {
                string errorMsg = "Parse kvpair failed, Invalid kvpairs: [" + originalStr + "]";
                AUTIL_LOG(WARN, "%s", errorMsg.c_str());
                return;
            } else {
                break;
            }
        }
        string value = getNextTerm(originalStr, kvPairSplit, start);
        setMap(key, value);
    }
}

} // namespace sql
