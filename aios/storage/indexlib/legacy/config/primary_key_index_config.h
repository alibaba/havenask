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

#include "autil/Log.h"
#include "indexlib/config/primary_key_load_strategy_param.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlibv2::index {
class DeletionMapConfig;
}

namespace indexlib { namespace config {

class PrimaryKeyIndexConfig : public SingleFieldIndexConfig
{
public:
    PrimaryKeyIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    PrimaryKeyIndexConfig(const PrimaryKeyIndexConfig& other);
    ~PrimaryKeyIndexConfig();

public:
    IndexConfig* Clone() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    void Check() const override;
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override;

public:
    void SetPrimaryKeyLoadParam(PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode, bool lookupReverse,
                                const std::string& param = "");
    PrimaryKeyLoadStrategyParamPtr GetPKLoadStrategyParam() const;
    void SetPrimaryKeyIndexType(PrimaryKeyIndexType type);
    PrimaryKeyIndexType GetPrimaryKeyIndexType() const;
    void SetPrimaryKeyHashType(index::PrimaryKeyHashType type);
    index::PrimaryKeyHashType GetPrimaryKeyHashType() const;
    void SetPrimaryKeyDataBlockSize(int32_t pkDataBlockSize);
    int32_t GetPrimaryKeyDataBlockSize() const;

    bool GetBloomFilterParamForPkReader(uint32_t& multipleNum, uint32_t& hashFuncNum) const;
    void EnableBloomFilterForPkReader(uint32_t multipleNum);
    bool IsParallelLookupOnBuild() const;
    void EnableParallelLookupOnBuild();

    // indexlibv2
    std::unique_ptr<indexlibv2::index::PrimaryKeyIndexConfig> MakePrimaryIndexConfigV2() const;

private:
    bool FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const override;

    bool IsPrimaryKeyIndex() const;
    void SetDefaultPrimaryKeyLoadParamFromPkIndexType();
    static PrimaryKeyIndexType StringToPkIndexType(const std::string& strPkIndexType);
    static std::string PkIndexTypeToString(const PrimaryKeyIndexType type);
    static index::PrimaryKeyHashType StringToPkHashType(const std::string& strPkHashType);
    static std::string PkHashTypeToString(const index::PrimaryKeyHashType type);

private:
    static constexpr int32_t DEFAULT_PK_DATA_BLOCK_SIZE = 4096;

private:
    bool CheckFieldType(FieldType ft) const override;

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PrimaryKeyIndexConfig> PrimaryKeyIndexConfigPtr;
}} // namespace indexlib::config
