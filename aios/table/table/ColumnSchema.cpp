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
#include "table/ColumnSchema.h"
#include "table/TableUtil.h"

using namespace std;

namespace table {

ColumnSchema::ColumnSchema(std::string name, ValueType type)
    : _name(name)
    , _type(type)
{
}

ColumnSchema::~ColumnSchema() {
}

std::string ColumnSchema::toString() const {
    return "name:" + _name + ",type:" + TableUtil::valueTypeToString(_type);
}

}
