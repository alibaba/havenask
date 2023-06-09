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

#include <optional>

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/util/Traits.h"
namespace indexlibv2::framework {

class IndexOperationDescription : public autil::legacy::Jsonizable
{
public:
    using Parameters = std::map<std::string, std::string>;
    IndexOperationDescription(IndexOperationId id, const IndexOperationType& type);
    IndexOperationDescription();
    ~IndexOperationDescription() {}

public:
    void Jsonize(JsonWrapper& json) override;

public:
    IndexOperationId GetId() const { return _id; }
    std::string GetOpFenceDirName() const;
    static std::string GetOpFenceDirName(IndexOperationId id);
    const IndexOperationType& GetType() const { return _type; }
    const Parameters& GetAllParameters() const { return _parameters; }
    template <typename T>
    bool GetParameter(const std::string& key, T& value) const;

    template <typename T>
    void AddParameter(const std::string& key, T value);

    const std::vector<IndexOperationId>& GetDepends() const { return _depends; }
    bool IsDependOn(IndexOperationId) const;
    void AddDepend(IndexOperationId id);
    void AddDepends(const std::vector<IndexOperationId>& ids);
    void SetEstimateMemory(int64_t estimateMemory) { _estimateMemUse = estimateMemory; }
    int64_t GetEstimateMemory() const { return _estimateMemUse; }
    bool operator==(const IndexOperationDescription& other) const;

public:
    // legacy code, compatible with old index task
    bool UseOpFenceDir() const;
    static constexpr char USE_OP_FENCE_DIR[] = "use_op_fence_dir";
    void ClearParameters() { _parameters.clear(); }

private:
    IndexOperationId _id;
    IndexOperationType _type;
    Parameters _parameters;
    int64_t _estimateMemUse = 1;
    std::vector<IndexOperationId> _depends;
};

template <typename T>
inline void IndexOperationDescription::AddParameter(const std::string& key, T value)
{
    if constexpr (std::is_same_v<T, std::string>) {
        _parameters[key] = autil::StringUtil::toString(value);
    } else if constexpr (util::HasTypedefIterator<T>::value) {
        _parameters[key] = autil::StringUtil::toString(value, indexlib::INDEXLIB_DEFAULT_DELIM_STR_LEGACY);
    } else {
        _parameters[key] = autil::StringUtil::toString(value);
    }
}

template <typename T>
bool IndexOperationDescription::GetParameter(const std::string& key, T& value) const
{
    auto iter = _parameters.find(key);
    if (iter == _parameters.end()) {
        return false;
    }
    if constexpr (std::is_same_v<T, std::string>) {
        return autil::StringUtil::fromString(iter->second, value);
    } else if constexpr (util::HasTypedefIterator<T>::value) {
        autil::StringUtil::fromString(iter->second, value, indexlib::INDEXLIB_DEFAULT_DELIM_STR_LEGACY);
        return true;
    } else {
        return autil::StringUtil::fromString(iter->second, value);
    }
}

} // namespace indexlibv2::framework
