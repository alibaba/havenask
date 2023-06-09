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
#include "indexlib/table/table_merge_plan_meta.h"

using namespace std;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, TableMergePlanMeta);

TableMergePlanMeta::TableMergePlanMeta() : locator(), timestamp(INVALID_TIMESTAMP), maxTTL(0), segmentDescription() {}

TableMergePlanMeta::~TableMergePlanMeta() {}

void TableMergePlanMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        string locatorString = autil::StringUtil::strToHexStr(locator.ToString());
        json.Jsonize("target_segment_locator", locatorString);
    } else {
        string locatorString;
        json.Jsonize("target_segment_locator", locatorString);
        locatorString = autil::StringUtil::hexStrToStr(locatorString);
        locator.SetLocator(locatorString);
    }
    json.Jsonize("target_segment_timestamp", timestamp);
    json.Jsonize("target_segment_maxTTL", maxTTL);
    json.Jsonize("target_segment_id", targetSegmentId);
    json.Jsonize("target_segment_description", segmentDescription);
}
}} // namespace indexlib::table
