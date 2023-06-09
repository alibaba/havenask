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
#include <iterator>

#include "StringUtil.h"

using namespace std;
namespace alog {

string StringUtil::trim(const string& str) {
    static const char* whiteSpace = " \t\r\n";

    if (str.empty())
        return str;

    string::size_type beginPos = str.find_first_not_of(whiteSpace);
    if (beginPos == string::npos)
        return "";

    string::size_type endPos = str.find_last_not_of(whiteSpace);

    return string(str, beginPos, endPos - beginPos + 1);
}

unsigned int StringUtil::split(vector<string>& vec,
                               const string& str,
                               char delimiter, unsigned int maxSegments)
{
    vec.clear();
    back_insert_iterator<vector<string> > it(vec);
    return split(it, str, delimiter, maxSegments);
}

}
