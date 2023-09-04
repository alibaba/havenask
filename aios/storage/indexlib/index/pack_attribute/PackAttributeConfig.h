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
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/attribute/Types.h"

namespace indexlib::config {
class CompressTypeOption;
class FileCompressConfig;
} // namespace indexlib::config

namespace indexlibv2::config {
class FileCompressConfigV2;
} // namespace indexlibv2::config

namespace indexlibv2::index {
class AttributeConfig;

class PackAttributeConfig : public config::IIndexConfig
{
    enum PackFormat {
        PF_DEFAULT, // 8 byte offset for var len, random access
        PF_IMPACT,  // impact offset for var len, random access
        PF_PLAIN,   // no offset for var len, sequence access
    };

public:
    PackAttributeConfig();
    ~PackAttributeConfig();

public:
    // override
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<config::FieldConfig>> GetFieldConfigs() const override;
    void Check() const override;
    Status CheckCompatible(const config::IIndexConfig* other) const override;
    bool IsDisabled() const override;

public:
    Status CheckEqual(const PackAttributeConfig& other) const;

    // Read
    const std::string& GetPackName() const;
    indexlib::config::CompressTypeOption GetCompressType() const;
    std::shared_ptr<indexlib::config::FileCompressConfig> GetFileCompressConfig() const;
    const std::shared_ptr<config::FileCompressConfigV2>& GetFileCompressConfigV2() const;
    packattrid_t GetPackAttrId() const;
    const std::vector<std::shared_ptr<AttributeConfig>>& GetAttributeConfigVec() const;
    void GetSubAttributeNames(std::vector<std::string>& attrNames) const;
    std::shared_ptr<AttributeConfig> CreateAttributeConfig() const;
    uint64_t GetDefragSlicePercent() const;
    // std::string GetPatchPath(const std::string& subAttrName) const;
    bool IsPackAttributeUpdatable() const;
    bool SupportNull() const;
    bool HasEnableImpact() const;
    bool HasEnablePlainFormat() const;

    // Write
    void SetPackName(const std::string& packName);
    void SetPackAttrId(packattrid_t packId);
    void SetUpdatable(bool updatable);
    void Disable();
    void EnableImpact();
    void EnablePlainFormat();
    void SetDefragSlicePercent(uint64_t defragSlicePercent);
    void SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& fileCompressConfig);
    void SetFileCompressConfigV2(const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfigV2);
    void SetCompressType(const indexlib::config::CompressTypeOption& compressType);
    Status SetCompressType(const std::string& compressStr);
    Status AddAttributeConfig(const std::shared_ptr<AttributeConfig>& attributeConfig);

private:
    Status AddAttributeConfig(const std::shared_ptr<AttributeConfig>& attributeConfig, bool isFromJsonMap);

public:
    inline static const std::string PACK_NAME = "pack_name";
    inline static const std::string SUB_ATTRIBUTES = "sub_attributes";
    inline static const std::string VALUE_FORMAT_IMPACT = "impact";
    inline static const std::string VALUE_FORMAT_PLAIN = "plain";
    inline static const std::string VALUE_FORMAT = "value_format";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
