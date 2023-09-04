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
#include "autil/codec/NormalizeTable.h"

#include <iosfwd>
#include <utility>

using namespace std;

namespace autil {
namespace codec {
AUTIL_LOG_SETUP(autil::codec, NormalizeTable);

NormalizeTable::NormalizeTable(bool caseSensitive,
                               bool traditionalSensitive,
                               bool widthSensitive,
                               const map<uint16_t, uint16_t> *traditionalTablePatch) {
    _table = new uint16_t[0x10000];
    if (!traditionalSensitive) {
        for (int i = 0; i < 0x10000; ++i) {
            _table[i] = CHN_T2S_SET[i];
        }
    } else {
        for (int i = 0; i < 0x10000; ++i) {
            _table[i] = i;
        }
    }

    if (!widthSensitive) {
        _table[0x3000] = ' ';
        for (int i = 0xFF01; i <= 0xFF5E; ++i) {
            _table[i] = i - 0xFEE0;
        }
    }

    if (!caseSensitive) {
        for (int i = 'A'; i <= 'Z'; ++i) {
            _table[i] += 'a' - 'A';
        }
        for (int i = 0xFF21; i <= 0xFF3a; ++i) {
            _table[i] += 'a' - 'A';
        }
    }

    if (!traditionalSensitive && traditionalTablePatch) {
        for (map<uint16_t, uint16_t>::const_iterator it = traditionalTablePatch->begin();
             it != traditionalTablePatch->end();
             ++it) {
            _table[it->first] = it->second;
        }
    }
}

NormalizeTable::~NormalizeTable() { delete[] _table; }

} // namespace codec
} // namespace autil
