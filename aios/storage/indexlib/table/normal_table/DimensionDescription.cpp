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
#include "indexlib/table/normal_table/DimensionDescription.h"

namespace indexlib::table {

std::string DimensionDescription::toString() const
{
    std::string str(name);
    str += " : { ranges : { ";
    for (const auto& range : ranges) {
        str += "( " + range.from + " , " + range.to + ") ";
    }
    str += "}; values : { ";
    for (const std::string& value : values) {
        str += value + ", ";
    }
    str += "}";
    return str;
}

} // namespace indexlib::table
