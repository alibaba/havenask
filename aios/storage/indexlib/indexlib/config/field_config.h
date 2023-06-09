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
#ifndef __INDEXLIB_FIELD_CONFIG_H
#define __INDEXLIB_FIELD_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

DECLARE_REFERENCE_CLASS(config, FieldConfigImpl);

namespace indexlib::test {
class SchemaMaker;
}

namespace indexlib { namespace config {

class FieldConfig : public indexlibv2::config::FieldConfig
{
public:
    friend class FieldSchema;

public:
    FieldConfig();
    FieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue);
    FieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue, bool isVirtual,
                bool isBinary = false);
    virtual ~FieldConfig() {}

public:
    void SetFieldType(FieldType type);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    virtual void AssertEqual(const FieldConfig& other) const;

    static FieldType StrToFieldType(const std::string& typeStr);
    static bool IsIntegerType(FieldType fieldType);

    void Check() const;

    FieldConfig* Clone() const;

    IndexStatus GetStatus() const;
    void Delete();
    bool IsDeleted() const;
    bool IsNormal() const;

    void SetOwnerModifyOperationId(schema_opid_t opId);
    schema_opid_t GetOwnerModifyOperationId() const;

    void RewriteFieldType();

    // optimize for attribute space
    const std::string& GetUserDefineAttributeNullValue() const;
    void SetUserDefineAttributeNullValue(const std::string& nullStr);

    static std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>>
    FieldConfigsToV2(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs)
    {
        std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> fieldConfigV2s;
        for (const auto& fieldConfig : fieldConfigs) {
            fieldConfigV2s.emplace_back(fieldConfig);
        }
        return fieldConfigV2s;
    }

private:
    // legacy interface, only for legacy AttributeConfig
    bool IsUpdatableMultiValue() const;
    CompressTypeOption GetCompressType() const;
    float GetDefragSlicePercent() const;
    void SetUpdatableMultiValue(bool IsUpdatableMultiValue);
    void SetCompressType(const std::string& compressStr);
    void SetUniqEncode(bool isUniqEncode);
    void SetDefragSlicePercent(uint64_t defragPercent);
    uint64_t GetU32OffsetThreshold();
    void SetU32OffsetThreshold(uint64_t offsetThreshold);
    void ClearCompressType();

public:
    // for test
    void SetMultiValue(bool multiValue);

private:
    void CheckUniqEncode() const;
    void CheckEquivalentCompress() const;
    void CheckBlockFpEncode() const;
    void CheckFp16Encode() const;
    void CheckFloatInt8Encode() const;
    void CheckUserDefineNullAttrValue() const;
    void CheckFieldTypeEqual(const FieldConfig& other) const;

private:
    friend class IndexPartitionSchemaMaker;
    friend class indexlib::test::SchemaMaker;
    friend class AttributeConfigImpl;
    friend class AttributeConfig;
    friend class PackAttributeConfig;
    friend class FieldConfigLoader;
    friend class RegionSchemaImpl;

    FieldConfigImplPtr mImpl;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FieldConfig);
typedef std::vector<FieldConfigPtr> FieldConfigVector;

class FieldConfigIterator
{
public:
    FieldConfigIterator(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs) : mConfigs(fieldConfigs) {}

    std::vector<std::shared_ptr<FieldConfig>>::const_iterator Begin() const { return mConfigs.begin(); }

    std::vector<std::shared_ptr<FieldConfig>>::const_iterator End() const { return mConfigs.end(); }

private:
    std::vector<std::shared_ptr<FieldConfig>> mConfigs;
};

DEFINE_SHARED_PTR(FieldConfigIterator);
}} // namespace indexlib::config

#endif //__INDEXLIB_FIELD_CONFIG_H
