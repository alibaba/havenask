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
#ifndef __INDEXLIB_UPDATEABLE_SCHEMA_STANDARDS_H
#define __INDEXLIB_UPDATEABLE_SCHEMA_STANDARDS_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/temperature_layer_config.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace config {

/*
  This is for schema some can updateable standards, such as: temperatureLayerConfig
 */
class UpdateableSchemaStandards : public autil::legacy::Jsonizable
{
public:
    UpdateableSchemaStandards() {}
    ~UpdateableSchemaStandards() {}

    UpdateableSchemaStandards(const UpdateableSchemaStandards& other)
        : mTableName(other.mTableName)
        , mLayerConfig(other.mLayerConfig)
        , mCompressConfig(other.mCompressConfig)
    {
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool IsEmpty() const;
    const config::TemperatureLayerConfigPtr& GetTemperatureLayerConfig() const { return mLayerConfig; }
    const std::shared_ptr<config::FileCompressSchema>& GetFileCompressSchema() const { return mCompressConfig; }
    const std::string& GetTableName() const { return mTableName; }
    bool CheckConfigValid(const IndexPartitionSchemaPtr& schema) const;

public:
    bool operator==(const UpdateableSchemaStandards& standards) const;

private:
    std::string mTableName;
    TemperatureLayerConfigPtr mLayerConfig;
    std::shared_ptr<FileCompressSchema> mCompressConfig;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(UpdateableSchemaStandards);

}} // namespace indexlib::config

#endif //__INDEXLIB_UPDATEABLE_SCHEMA_STANDARDS_H
