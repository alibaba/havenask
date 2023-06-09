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
class AttributeConfig;

class PackAttributeConfig : public IIndexConfig
{
    enum PackFormat {
        PF_DEFAULT, // 8 byte offset for var len, random access
        PF_IMPACT,  // impact offset for var len, random access
        PF_PLAIN,   // no offset for var len, sequence access
    };

public:
    PackAttributeConfig(const std::string& attrName, const indexlib::config::CompressTypeOption& compressType,
                        uint64_t defragSlicePercent,
                        const std::shared_ptr<indexlib::config::FileCompressConfig>& fileCompressConfig);
    ~PackAttributeConfig();

public:
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override
    {
    }
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;
    void Check() const override;
    Status CheckCompatible(const IIndexConfig* other) const override;

public:
    virtual Status AddAttributeConfig(const std::shared_ptr<AttributeConfig>& attributeConfig);

public:
    const std::string& GetAttrName() const;
    indexlib::config::CompressTypeOption GetCompressType() const;
    // TODO(tianxiao) need to  support GetFileCompressConfigV2 when pack attribute ready
    std::shared_ptr<indexlib::config::FileCompressConfig> GetFileCompressConfig() const;
    packattrid_t GetPackAttrId() const;
    void SetPackAttrId(packattrid_t packId);
    const std::vector<std::shared_ptr<AttributeConfig>>& GetAttributeConfigVec() const;
    void GetSubAttributeNames(std::vector<std::string>& attrNames) const;

    Status CheckEqual(const PackAttributeConfig& other) const;
    std::shared_ptr<AttributeConfig> CreateAttributeConfig() const;
    bool IsUpdatable() const;
    bool SupportNull() const;
    void Disable();
    bool IsDisable() const;
    bool HasEnableImpact() const;
    void EnableImpact();
    bool HasEnablePlainFormat() const;
    void EnablePlainFormat();
    uint64_t GetDefragSlicePercent() const;

public:
    inline static const std::string PACK_ATTR_NAME_FIELD = "pack_name";
    inline static const std::string PACK_ATTR_SUB_ATTR_FIELD = "sub_attributes";
    // inline static const std::string PACK_ATTR_COMPRESS_TYPE_FIELD = "compress_type";
    inline static const std::string PACK_ATTR_VALUE_FORMAT_IMPACT = "impact";
    inline static const std::string PACK_ATTR_VALUE_FORMAT_PLAIN = "plain";
    inline static const std::string PACK_ATTR_VALUE_FORMAT = "value_format";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
