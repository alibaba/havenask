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
#include "indexlib/common/term_hint_parser.h"

using namespace std;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, TermHintParser);

TermHintParser::TermHintParser() {}

TermHintParser::~TermHintParser() {}

// last bit is mark HOT
// second last bit is mark WARM
// third last bit is mark COLD
int32_t TermHintParser::GetHintValues(const std::vector<std::string>& inputs)
{
    int32_t hintValues = 0;
    for (const string& input : inputs) {
        if (input == "HOT") {
            hintValues |= 1;
        } else if (input == "WARM") {
            hintValues |= (1 << 1);
        } else if (input == "COLD") {
            hintValues |= (1 << 2);
        } else {
            IE_LOG(WARN, "not support hint value [%s]", input.c_str());
        }
    }
    return hintValues;
}

vector<index::TemperatureProperty> TermHintParser::GetHintProperty(int32_t hintValues)
{
    vector<index::TemperatureProperty> ret;
    if (hintValues == -1) {
        return ret;
    }
    if ((hintValues & 1) != 0) {
        ret.push_back(index::TemperatureProperty::HOT);
    }
    if ((hintValues & (1 << 1)) != 0) {
        ret.push_back(index::TemperatureProperty::WARM);
    }
    if ((hintValues & (1 << 2)) != 0) {
        ret.push_back(index::TemperatureProperty::COLD);
    }
    return ret;
}

}} // namespace indexlib::common
