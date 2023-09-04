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
#ifndef __INDEXLIB_ATTRIBUTE_CONFIG_H
#define __INDEXLIB_ATTRIBUTE_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(config, FileCompressConfig);
DECLARE_REFERENCE_CLASS(common, AttributeValueInitializerCreator);

namespace indexlib { namespace config {

class AttributeConfig final : public autil::legacy::Jsonizable, public indexlibv2::index::AttributeConfig
{
public:
    enum ConfigType {
        ct_normal,
        ct_virtual,
        ct_section,
        ct_pk,
        ct_unknown,
        ct_index_accompany,
        ct_summary_accompany,
    };

public:
    AttributeConfig(ConfigType type = ct_normal);
    virtual ~AttributeConfig();

public:
    void
    Init(const config::FieldConfigPtr& fieldConfig,
         const common::AttributeValueInitializerCreatorPtr& creator = common::AttributeValueInitializerCreatorPtr(),
         const config::CustomizedConfigVector& customizedConfigs = config::CustomizedConfigVector());
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    size_t EstimateAttributeExpandSize(size_t docCount);

    FieldConfigPtr GetFieldConfig() const;
    bool IsBuiltInAttribute() const;
    common::AttributeValueInitializerCreatorPtr GetAttrValueInitializerCreator() const;
    void AssertEqual(const AttributeConfig& other) const;
    void SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs);
    const CustomizedConfigVector& GetCustomizedConfigs() const;

    PackAttributeConfig* GetPackAttributeConfig() const;

    void SetOwnerModifyOperationId(schema_opid_t opId);
    schema_opid_t GetOwnerModifyOperationId() const;

    void SetUpdatableMultiValue(bool updatable);
    ConfigType GetConfigType() const;
    void SetConfigType(ConfigType type);

public:
    Status SetCompressType(const std::string& compressStr) override;
    bool IsLegacyAttributeConfig() const override;

private:
    void SetFieldConfig(const FieldConfigPtr& fieldConfig);

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeConfig);
typedef std::vector<AttributeConfigPtr> AttributeConfigVector;

class AttributeConfigIterator
{
public:
    AttributeConfigIterator(const AttributeConfigVector& configs) : mConfigs(configs) {}

    AttributeConfigVector::const_iterator Begin() const { return mConfigs.begin(); }

    AttributeConfigVector::const_iterator End() const { return mConfigs.end(); }

private:
    AttributeConfigVector mConfigs;
};
DEFINE_SHARED_PTR(AttributeConfigIterator);
}} // namespace indexlib::config

#endif //__INDEXLIB_ATTRIBUTE_CONFIG_H
