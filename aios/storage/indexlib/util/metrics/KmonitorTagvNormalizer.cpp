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
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"

using namespace std;

namespace indexlib { namespace util {

KmonitorTagvNormalizer::KmonitorTagvNormalizer() : _bitmap((uint32_t)256) { InitValidCharBitmap(); }

KmonitorTagvNormalizer::~KmonitorTagvNormalizer() {}

void KmonitorTagvNormalizer::InitValidCharBitmap()
{
#define SET_CHAR_VALID(x) _bitmap.Set((uint8_t)x)

    for (char a = '0'; a <= '9'; a++) {
        SET_CHAR_VALID(a);
    }
    for (char a = 'a'; a <= 'z'; a++) {
        SET_CHAR_VALID(a);
    }
    for (char a = 'A'; a <= 'Z'; a++) {
        SET_CHAR_VALID(a);
    }
    SET_CHAR_VALID('\0');
    SET_CHAR_VALID('.');
    SET_CHAR_VALID('-');
    SET_CHAR_VALID('_');
}

string KmonitorTagvNormalizer::Normalize(const string& str, char rep)
{
    if (!IsValidChar(rep)) {
        rep = '_';
    }
    char* data = (char*)str.data();
    vector<char> strVec;
    for (size_t i = 0; i < str.size(); i++) {
        if (IsValidChar(data[i])) {
            continue;
        }
        if (strVec.empty()) {
            strVec.resize(str.size());
            memcpy((void*)strVec.data(), str.data(), str.size());
        }
        strVec[i] = rep;
    }
    return strVec.empty() ? str : string(strVec.begin(), strVec.end());
}

}} // namespace indexlib::util
