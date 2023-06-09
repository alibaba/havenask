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

#include <stdint.h>
#include <map>

#include "autil/Log.h"

namespace autil {
namespace codec {

class NormalizeTable
{
public:
    NormalizeTable(bool caseSensitive,
                   bool traditionalSensitive,
                   bool widthSensitive,
                   const std::map<uint16_t, uint16_t> *traditionalTablePatch);
    ~NormalizeTable();
private:
    NormalizeTable(const NormalizeTable &rhs);
    NormalizeTable& operator= (const NormalizeTable &rhs);
public:
    uint16_t operator[] (uint16_t u) const {
        return _table[u];
    }
private:
    uint16_t *_table;
private:
    static const uint16_t CHN_T2S_SET[0x10000];
private:
    AUTIL_LOG_DECLARE();
};

}
}

