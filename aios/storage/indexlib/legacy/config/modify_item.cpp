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
#include "indexlib/config/modify_item.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, ModifyItem);

ModifyItem::ModifyItem() : mOpId(INVALID_SCHEMA_OP_ID) {}

ModifyItem::ModifyItem(const string& name, const string& type, schema_opid_t opId)
    : mName(name)
    , mType(type)
    , mOpId(opId)
{
}

ModifyItem::~ModifyItem() {}

void ModifyItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("name", mName);
    json.Jsonize("type", mType);

    if (json.GetMode() == TO_JSON) {
        if (mOpId != INVALID_SCHEMA_OP_ID) {
            json.Jsonize("operation_id", mOpId);
        }
    } else {
        json.Jsonize("operation_id", mOpId, mOpId);
    }
}
}} // namespace indexlib::config
