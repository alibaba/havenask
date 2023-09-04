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
#include "indexlib/index/operation_log/OperationLogConfig.h"

#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/index/primary_key/Common.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::FieldConfig;
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::SingleFieldIndexConfig;
} // namespace

size_t OperationLogConfig::GetMaxBlockSize() const { return _maxBlockSize; }

bool OperationLogConfig::IsCompress() const { return _isCompress; }
const std::string& OperationLogConfig::GetIndexName() const { return OPERATION_LOG_INDEX_NAME; }

std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> OperationLogConfig::GetPrimaryKeyIndexConfig() const
{
    auto iter = _indexConfigs.find(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (iter == _indexConfigs.end()) {
        static std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> nullConfig;
        return nullConfig;
    }
    assert(iter->second.size() == 1);
    return std::dynamic_pointer_cast<indexlibv2::config::SingleFieldIndexConfig>((iter->second)[0]);
}

bool OperationLogConfig::HasIndexConfig(const std::string& indexType, const std::string& indexName)
{
    auto iter = _indexConfigs.find(indexType);
    if (iter == _indexConfigs.end()) {
        return false;
    }
    auto indexConfigs = iter->second;
    for (auto indexConfig : indexConfigs) {
        if (indexConfig->GetIndexType() == indexType && indexConfig->GetIndexName() == indexName) {
            return true;
        }
    }
    return false;
}

const std::string& OperationLogConfig::GetIndexType() const { return OPERATION_LOG_INDEX_TYPE_STR; }
const std::string& OperationLogConfig::GetIndexCommonPath() const { return OPERATION_LOG_INDEX_PATH; }
std::vector<std::string> OperationLogConfig::GetIndexPath() const { return {GetIndexCommonPath()}; }
void OperationLogConfig::AddIndexConfigs(const std::string& indexType,
                                         const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs)
{
    if (!indexConfigs.empty()) {
        _indexConfigs[indexType] = indexConfigs;
    }
    decltype(_usingFieldConfigs) usingFieldConfigs;
    for (const auto& [type, indexConfigs] : _indexConfigs) {
        for (const auto& indexConfig : indexConfigs) {
            bool useful = false;
            if (ATTRIBUTE_INDEX_TYPE_STR == indexConfig->GetIndexType()) {
                auto typedConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
                if (!typedConfig || typedConfig->IsAttributeUpdatable()) {
                    // we assume this case success, make it useful if failed
                    useful = true;
                }
            } else if (INVERTED_INDEX_TYPE_STR == indexConfig->GetIndexType()) {
                auto typedConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
                if (!typedConfig || typedConfig->IsIndexUpdatable()) {
                    // we assume this case success, make it useful if failed
                    useful = true;
                }
            }
            if (useful) {
                auto usingConfigs = indexConfig->GetFieldConfigs();
                usingFieldConfigs.insert(usingFieldConfigs.end(), usingConfigs.begin(), usingConfigs.end());
            }
        }
    }
    usingFieldConfigs.erase(
        std::unique(usingFieldConfigs.begin(), usingFieldConfigs.end(),
                    [](const auto& a, const auto& b) { return a->GetFieldName() == b->GetFieldName(); }),
        usingFieldConfigs.end());
    _usingFieldConfigs = usingFieldConfigs;
}
void OperationLogConfig::SetFieldConfigs(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs)
{
    _allFieldConfigs = fieldConfigs;
}

const std::vector<std::shared_ptr<IIndexConfig>>&
OperationLogConfig::GetIndexConfigs(const std::string& indexType) const
{
    auto iter = _indexConfigs.find(indexType);
    if (iter == _indexConfigs.end()) {
        static std::vector<std::shared_ptr<IIndexConfig>> emptyIndexConfigs;
        return emptyIndexConfigs;
    }
    return iter->second;
}

std::vector<std::shared_ptr<FieldConfig>> OperationLogConfig::GetFieldConfigs() const { return _usingFieldConfigs; }
std::vector<std::shared_ptr<FieldConfig>> OperationLogConfig::GetAllFieldConfigs() const { return _allFieldConfigs; }

Status OperationLogConfig::CheckCompatible(const IIndexConfig* other) const { return Status::OK(); }
bool OperationLogConfig::IsDisabled() const { return false; }

} // namespace indexlib::index
