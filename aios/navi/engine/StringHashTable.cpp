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
#include "navi/engine/StringHashTable.h"

namespace navi {

StringHashTable::StringHashTable() {
}

StringHashTable::~StringHashTable() {
}

size_t StringHashTable::getHash(const std::string &str) {
    auto value = _hashFunc(str);
    auto tableMapDef = _fullTable.mutable_table();
    auto it = tableMapDef->find(value);
    if (tableMapDef->end() == it) {
        (*_incTable.mutable_table())[value] = str;
    }
    return value;
}

void StringHashTable::toProto(SymbolTableDef *symbolTableDef) {
    symbolTableDef->CopyFrom(_incTable);
    _fullTable.MergeFrom(_incTable);
    _incTable.Clear();
}

}

