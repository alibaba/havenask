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

#include <memory>

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/primary_key/Types.h"
#include "indexlib/index/primary_key/config/PrimaryKeyLoadStrategyParam.h"

namespace indexlibv2::index {
class DeletionMapConfig;

class PrimaryKeyIndexConfig : public config::SingleFieldIndexConfig
{
public:
    static constexpr int32_t DEFAULT_BLOOM_FILTER_MULTIPLE_NUM = 5;

public:
    PrimaryKeyIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    PrimaryKeyIndexConfig(const PrimaryKeyIndexConfig& other);
    ~PrimaryKeyIndexConfig();
    PrimaryKeyIndexConfig& operator=(const PrimaryKeyIndexConfig& other);

public:
    PrimaryKeyIndexConfig* Clone() const override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckEqual(const InvertedIndexConfig& other) const override;
    void Check() const override;

public:
    void DisablePrimaryKeyCombineSegments();
    Status SetPrimaryKeyLoadParam(indexlib::config::PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
                                  const std::string& param = "");
    void SetPrimaryKeyLoadParam(const indexlib::config::PrimaryKeyLoadStrategyParam& param);
    const indexlib::config::PrimaryKeyLoadStrategyParam& GetPKLoadStrategyParam() const;
    void SetPrimaryKeyIndexType(PrimaryKeyIndexType type);
    PrimaryKeyIndexType GetPrimaryKeyIndexType() const;
    void SetPrimaryKeyHashType(PrimaryKeyHashType type);
    PrimaryKeyHashType GetPrimaryKeyHashType() const;
    void SetPrimaryKeyDataBlockSize(int32_t pkDataBlockSize);
    int32_t GetPrimaryKeyDataBlockSize() const;

    bool GetBloomFilterParamForPkReader(uint32_t& multipleNum, uint32_t& hashFuncNum) const;
    void EnableBloomFilterForPkReader(uint32_t multipleNum);
    bool IsParallelLookupOnBuild() const;
    void EnableParallelLookupOnBuild();
    std::shared_ptr<config::AttributeConfig> GetPKAttributeConfig() const;
    void SetPrimaryKeyAttributeFlag(bool flag);
    bool HasPrimaryKeyAttribute() const;

protected:
    void DoDeserialize(const autil::legacy::Any& any, const config::IndexConfigDeserializeResource& resource) override;

public:
    std::optional<indexlib::config::PrimaryKeyLoadStrategyParam> TEST_GetPrimaryKeyLoadStrategyParam() const;

private:
    bool IsPrimaryKeyIndex() const;
    void SetDefaultPrimaryKeyLoadParamFromPkIndexType();

    // throws in Jsonize
    static PrimaryKeyIndexType StringToPkIndexType(const std::string& strPkIndexType);
    static std::string PkIndexTypeToString(const PrimaryKeyIndexType type);
    static PrimaryKeyHashType StringToPkHashType(const std::string& strPkHashType);
    static std::string PkHashTypeToString(const PrimaryKeyHashType type);

private:
    static constexpr int32_t DEFAULT_PK_DATA_BLOCK_SIZE = 4096;

private:
    bool CheckFieldType(FieldType ft) const override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
