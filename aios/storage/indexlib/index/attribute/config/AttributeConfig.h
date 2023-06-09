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
#pragma once
#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/common/Types.h"

namespace indexlib::config {
class FileCompressConfig;
} // namespace indexlib::config

namespace indexlibv2::config {
class PackAttributeConfig;
class FileCompressConfigV2;

class AttributeConfig : public IIndexConfig
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
    ~AttributeConfig();

public:
    void Init(const std::shared_ptr<FieldConfig>& fieldConfig);
    Status AssertEqual(const AttributeConfig& other) const;

    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    void Check() const override;
    Status CheckCompatible(const IIndexConfig* other) const override;

public:
    // Read
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    std::vector<std::string> GetIndexPath() const override; //获取index从segment开始的相对路径
    std::string GetSliceDir() const;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;

    const std::shared_ptr<FieldConfig>& GetFieldConfig() const;
    const std::string& GetAttrName() const;
    attrid_t GetAttrId() const;
    indexlib::config::CompressTypeOption GetCompressType() const;
    ConfigType GetConfigType() const;
    float GetDefragSlicePercent();
    fieldid_t GetFieldId() const;
    const std::shared_ptr<indexlib::config::FileCompressConfig>& GetFileCompressConfig() const;
    const std::shared_ptr<FileCompressConfigV2>& GetFileCompressConfigV2() const;
    PackAttributeConfig* GetPackAttributeConfig() const;
    bool IsAttributeUpdatable() const;
    bool IsLengthFixed() const;
    bool IsLoadPatchExpand() const;
    bool IsUniqEncode() const;
    bool SupportNull() const;
    bool IsMultiValue() const;
    FieldType GetFieldType() const;
    int32_t GetFixedMultiValueCount() const;
    uint32_t GetFixLenFieldSize() const;
    bool IsMultiString() const;
    uint64_t GetU32OffsetThreshold() const;
    bool IsDisable() const;
    bool IsDeleted() const;
    bool IsNormal() const;
    indexlib::IndexStatus GetStatus() const;
    int64_t GetSliceCount() const;
    int64_t GetSliceIdx() const;
    std::vector<std::shared_ptr<AttributeConfig>> CreateSliceAttributeConfigs(int64_t sliceCount);

public:
    // Write
    void SetUpdatable(bool isUpdatable);
    void SetAttrId(attrid_t id);
    void SetConfigType(ConfigType type);
    void SetPackAttributeConfig(PackAttributeConfig* packAttrConfig);
    void SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& fileCompressConfig);
    void SetFileCompressConfigV2(const std::shared_ptr<FileCompressConfigV2>& fileCompressConfigV2);
    void SetU32OffsetThreshold(uint64_t offsetThreshold);
    void SetDefragSlicePercent(uint64_t percent);
    void Disable();
    Status Delete();

public:
    virtual bool IsLegacyAttributeConfig() const;
    virtual Status SetCompressType(const std::string& compressStr);

public:
    void TEST_ClearCompressType();

private:
    std::shared_ptr<AttributeConfig> Clone();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
