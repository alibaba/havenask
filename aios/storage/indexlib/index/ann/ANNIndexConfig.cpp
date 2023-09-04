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
#include "indexlib/index/ann/ANNIndexConfig.h"

#include "indexlib/config/ConfigDefine.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, ANNIndexConfig);

struct ANNIndexConfig::Impl {
    std::string indexerName;
    indexlib::util::KeyValueMap params;
};

ANNIndexConfig::ANNIndexConfig(const string& indexName, InvertedIndexType indexType)
    : PackageIndexConfig(indexName, indexType)
    , _impl(make_unique<Impl>())
{
    // attention: set property format version id according to function log
    [[maybe_unused]] auto status = SetMaxSupportedIndexFormatVersionId(0);
}

ANNIndexConfig::ANNIndexConfig(const ANNIndexConfig& other)
    : PackageIndexConfig(other)
    , _impl(make_unique<Impl>(*(other._impl)))
{
}

ANNIndexConfig::~ANNIndexConfig() {}

void ANNIndexConfig::Check() const
{
    if (SupportNull()) {
        INDEXLIB_FATAL_ERROR(Schema, "customize index [%s] not support use field which enable null",
                             GetIndexName().c_str());
    }
}

void ANNIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    PackageIndexConfig::Serialize(json);
    json.Jsonize(ANN_INDEXER_NAME, _impl->indexerName, _impl->indexerName);
    json.Jsonize(ANN_INDEXER_PARAMS, _impl->params);
}

void ANNIndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                   const config::IndexConfigDeserializeResource& resource)
{
    PackageIndexConfig::DoDeserialize(any, resource);
    autil::legacy::Jsonizable::JsonWrapper json(any);
    json.Jsonize(ANN_INDEXER_NAME, _impl->indexerName, _impl->indexerName);
    map<string, autil::legacy::Any> data = json.GetMap();
    auto iter = data.find(ANN_INDEXER_PARAMS);
    if (iter != data.end()) {
        map<string, autil::legacy::Any> paramMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(iter->second);
        auto paramIter = paramMap.begin();
        for (; paramIter != paramMap.end(); paramIter++) {
            string value;
            if (paramIter->second.GetType() == typeid(string)) {
                value = autil::legacy::AnyCast<string>(paramIter->second);
            } else {
                autil::legacy::json::ToString(paramIter->second, value, true, "");
            }
            _impl->params.insert(make_pair(paramIter->first, value));
        }
    }
}

Status ANNIndexConfig::CheckEqual(const InvertedIndexConfig& other) const
{
    auto status = PackageIndexConfig::CheckEqual(other);
    RETURN_IF_STATUS_ERROR(status, "date customized config check equal fail, indexName[%s]", GetIndexName().c_str());
    const ANNIndexConfig& typedOther = (const ANNIndexConfig&)other;
    CHECK_CONFIG_EQUAL(_impl->indexerName, typedOther._impl->indexerName, "customized indexer name not equal");
    CHECK_CONFIG_EQUAL(_impl->params, typedOther._impl->params, "indexer parameters not equal");
    return Status::OK();
}

InvertedIndexConfig* ANNIndexConfig::Clone() const { return new ANNIndexConfig(*this); }

const std::string& ANNIndexConfig::GetIndexerName() const { return _impl->indexerName; }

const indexlib::util::KeyValueMap& ANNIndexConfig::GetParameters() const { return _impl->params; }

void ANNIndexConfig::SetIndexer(const std::string& indexerName) { _impl->indexerName = indexerName; }

void ANNIndexConfig::SetParameters(const indexlib::util::KeyValueMap& params) { _impl->params = params; }

bool ANNIndexConfig::CheckFieldType(FieldType ft) const
{
    // support all field type
    return true;
}

} // namespace indexlibv2::config
