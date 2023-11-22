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
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"

namespace indexlib::index {

/*
 "field_meta" : [
          {
            "field_name" : "integer1",
            "index_name" :  "integer1_meta",
            "metas" : [
                {
                    "metaName" : "MinMax"
                },
                {
                     "metaName" : "DataStatistics"
                     "metaParams" : {
                         "top_num" : 20
                     }
                },
                {
                     "metaName" : "Histogram"
                     "metaParams" : {
                         "bins" : 20,
                         "max_value" : 100,
                         "min_value" : -100
                     }
                },
            }
            ],
            "store_meta_source" : false
          }
  ]
 */
class FieldMetaConfig : public indexlibv2::config::IIndexConfig
{
public:
    enum class MetaSourceType {
        MST_FIELD,
        MST_TOKEN_COUNT,
        MST_NONE,
    };
    struct FieldMetaInfo : public autil::legacy::Jsonizable {
        FieldMetaInfo() {}
        FieldMetaInfo(const std::string& metaNameStr) : metaName(metaNameStr) {}
        std::string metaName;
        autil::legacy::json::JsonMap metaParams;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("metaName", metaName, metaName);
            json.Jsonize("metaParams", metaParams, metaParams);
        }
    };
    typedef std::vector<FieldMetaInfo> FieldMetaInfoVec;

public:
    static constexpr const char* BINS = "bins";
    static constexpr const char* MAX_VALUE = "max_value";
    static constexpr const char* MIN_VALUE = "min_value";
    static constexpr const char* TOP_NUM = "top_num";

public:
    FieldMetaConfig();
    ~FieldMetaConfig();

public:
    Status Init(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig, const std::string& indexName);

public:
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const indexlibv2::config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    void Check() const override;
    Status CheckCompatible(const indexlibv2::config::IIndexConfig* other) const override;
    bool IsDisabled() const override;

    bool SupportTokenCount() const;
    MetaSourceType GetStoreMetaSourceType() const;
    std::string GetSourceFieldName() const;

public:
    const std::vector<FieldMetaInfo>& GetFieldMetaInfos() const;
    std::shared_ptr<indexlibv2::config::FieldConfig> GetFieldConfig() const;

public:
    std::vector<FieldMetaInfo>& TEST_GetFieldMetaInfos();
    void TEST_SetStoreMetaSourceType(MetaSourceType type);

private:
    template <typename T>
    void CheckHistogramMinMaxConfig(const FieldMetaConfig::FieldMetaInfo& info) const;

    void CheckFieldMetaParam(const FieldMetaConfig::FieldMetaInfo& info) const;
    std::vector<std::string> GetSupportMetas(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig) const;
    bool NeedFieldSource() const;
    bool NeedTokenCount() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
