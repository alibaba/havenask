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
#ifndef __INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_H
#define __INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class SingleFieldIndexConfig : public IndexConfig
{
public:
    SingleFieldIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    SingleFieldIndexConfig(const SingleFieldIndexConfig& other);
    ~SingleFieldIndexConfig();

public:
    uint32_t GetFieldCount() const override;
    void Check() const override;
    bool IsInIndex(fieldid_t fieldId) const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override;

    IndexConfig::Iterator CreateIterator() const override;
    int32_t GetFieldIdxInPack(fieldid_t id) const override;
    util::KeyValueMap GetDictHashParams() const override;

protected:
    bool CheckFieldType(FieldType ft) const override;
    void CheckWhetherIsVirtualField() const;
    bool FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const override;

public:
    void SetFieldConfig(const FieldConfigPtr& fieldConfig);
    FieldConfigPtr GetFieldConfig() const;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const override;

    // TODO: outsize used in plugin, expect merge to primarykey config
    bool HasPrimaryKeyAttribute() const;
    void SetPrimaryKeyAttributeFlag(bool flag);

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    friend class SingleFieldIndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldIndexConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_H
