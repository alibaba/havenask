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
#include "indexlib/config/updateable_schema_standards.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, UpdateableSchemaStandards);

void UpdateableSchemaStandards::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("table_name", mTableName);
    if (json.GetMode() == TO_JSON) {
        if (mLayerConfig) {
            json.Jsonize(TEMPERATURE_LAYER_CONFIG, mLayerConfig);
        }
        if (mCompressConfig) {
            mCompressConfig->Jsonize(json);
        }
    } else {
        std::map<std::string, Any> jsonMap = json.GetMap();
        json.Jsonize(TEMPERATURE_LAYER_CONFIG, mLayerConfig, mLayerConfig);
        auto iter = jsonMap.find(FILE_COMPRESS);
        if (iter != jsonMap.end()) {
            mCompressConfig.reset(FileCompressSchema::FromJson(iter->second));
        }
    }
}

bool UpdateableSchemaStandards::CheckConfigValid(const IndexPartitionSchemaPtr& schema) const
{
    try {
        if (mLayerConfig) {
            mLayerConfig->Check(schema->GetAttributeSchema());
        }
        if (mCompressConfig) {
            mCompressConfig->Check();
        }
    } catch (...) {
        IE_LOG(ERROR, "update schema patch is invalid");
        return false;
    }
    return true;
}

bool UpdateableSchemaStandards::IsEmpty() const { return mLayerConfig == nullptr && mCompressConfig == nullptr; }

bool UpdateableSchemaStandards::operator==(const UpdateableSchemaStandards& standards) const
{
    if (mTableName != standards.mTableName) {
        return false;
    }

#define CHECK_ITEM_EQUAL(itemPtr)                                                                                      \
    if (itemPtr == nullptr && standards.itemPtr != nullptr) {                                                          \
        return false;                                                                                                  \
    }                                                                                                                  \
    if (itemPtr != nullptr && standards.itemPtr == nullptr) {                                                          \
        return false;                                                                                                  \
    }                                                                                                                  \
    if (itemPtr != nullptr && standards.itemPtr != nullptr && *itemPtr != *standards.itemPtr) {                        \
        return false;                                                                                                  \
    }

    CHECK_ITEM_EQUAL(mCompressConfig);
    CHECK_ITEM_EQUAL(mLayerConfig);
    return true;
}

}} // namespace indexlib::config
