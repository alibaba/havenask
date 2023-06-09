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
#include "build_service/reader/IndexQueryCondition.h"

using namespace std;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, IndexQueryCondition);

const std::string IndexQueryCondition::AND_CONDITION_TYPE = "AND";
const std::string IndexQueryCondition::OR_CONDITION_TYPE = "OR";
const std::string IndexQueryCondition::TERM_CONDITION_TYPE = "TERM";

void IndexQueryCondition::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("type", mConditionType, TERM_CONDITION_TYPE);
    if (json.GetMode() == FROM_JSON) {
        if (mConditionType == TERM_CONDITION_TYPE) {
            IndexTermInfo info;
            json.Jsonize("index_info", info);
            mIndexTermInfos.push_back(info);
        } else {
            json.Jsonize("index_info", mIndexTermInfos);
        }
    } else {
        if (mConditionType == TERM_CONDITION_TYPE) {
            IndexTermInfo info;
            if (!mIndexTermInfos.empty()) {
                info = mIndexTermInfos[0];
            }
            json.Jsonize("index_info", info);
        } else {
            json.Jsonize("index_info", mIndexTermInfos);
        }
    }
}

}} // namespace build_service::reader
