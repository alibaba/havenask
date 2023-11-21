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
#include "autil/NoCopyable.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/field_format/field_meta/FieldMetaConvertor.h"
#include "indexlib/index/field_meta/Common.h"

namespace indexlib::index {

template <typename T>
class MinMaxFieldMeta : public IFieldMeta
{
public:
    MinMaxFieldMeta(const FieldMetaConfig::FieldMetaInfo& info) : IFieldMeta(info) {}

    ~MinMaxFieldMeta() {}
    MinMaxFieldMeta(const MinMaxFieldMeta& other)
        : _minValue(other._minValue.load())
        , _maxValue(other._maxValue.load())
        , _hasValidData(other._hasValidData)
    {
    }

public:
    MinMaxFieldMeta<T>* Clone() override;
    Status Build(const FieldValueBatch& fieldValues) override;
    Status Store(autil::mem_pool::PoolBase* dumpPool,
                 const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;

    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status Merge(const std::shared_ptr<IFieldMeta>& other) override;
    void GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                   int64_t& dumpFileSize) override;

    T GetMaxValue() const;
    T GetMinValue() const;

private:
    std::atomic<T> _minValue = std::numeric_limits<T>::max();
    std::atomic<T> _maxValue = std::numeric_limits<T>::min();

    bool _hasValidData = false;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MinMaxFieldMeta, T);
/////////////////////////////////////////////////////////

template <typename T>
void MinMaxFieldMeta<T>::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == FROM_JSON) {
        T minValue = std::numeric_limits<T>::max();
        T maxValue = std::numeric_limits<T>::min();
        std::string minStr, maxStr;
        json.Jsonize("min_value", minStr, "");
        json.Jsonize("max_value", maxStr, "");
        if (!minStr.empty()) {
            [[maybe_unused]] bool ret = autil::StringUtil::fromString(minStr, minValue);
            assert(ret);
        }
        if (!maxStr.empty()) {
            [[maybe_unused]] bool ret = autil::StringUtil::fromString(maxStr, maxValue);
            assert(ret);
        }
        json.Jsonize("has_valid_data", _hasValidData, false);
        _minValue.store(minValue);
        _maxValue.store(maxValue);

    } else if (json.GetMode() == TO_JSON) {
        json.Jsonize("min_value", std::to_string(_minValue.load()));
        json.Jsonize("max_value", std::to_string(_maxValue.load()));
        json.Jsonize("has_valid_data", _hasValidData);
    }
}

template <typename T>
Status MinMaxFieldMeta<T>::Merge(const std::shared_ptr<IFieldMeta>& other)
{
    auto minMaxFieldMeta = std::dynamic_pointer_cast<MinMaxFieldMeta<T>>(other);
    if (!minMaxFieldMeta) {
        assert(false);
        return Status::Corruption("cast min max field meta failed");
    }
    _maxValue.store(std::max(_maxValue.load(), minMaxFieldMeta->GetMaxValue()));
    _minValue.store(std::min(_minValue.load(), minMaxFieldMeta->GetMinValue()));
    _hasValidData |= minMaxFieldMeta->_hasValidData;
    return Status::OK();
}

template <typename T>
Status MinMaxFieldMeta<T>::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    std::string fieldMeta;
    auto status = directory
                      ->Load(MIN_MAX_META_FILE_NAME,
                             indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), fieldMeta)
                      .Status();
    RETURN_IF_STATUS_ERROR(status, "load min max field meta from [%s] failed", directory->GetPhysicalPath("").c_str());
    FromJsonString(*this, fieldMeta);
    return Status::OK();
}

template <typename T>
T MinMaxFieldMeta<T>::GetMaxValue() const
{
    return _maxValue.load();
}

template <typename T>
T MinMaxFieldMeta<T>::GetMinValue() const
{
    return _minValue.load();
}

template <typename T>
MinMaxFieldMeta<T>* MinMaxFieldMeta<T>::Clone()
{
    return new MinMaxFieldMeta(*this);
}

template <typename T>
Status MinMaxFieldMeta<T>::Build(const FieldValueBatch& fieldValues)
{
    for (const auto& [fieldValueMeta, isNull, _] : fieldValues) {
        const auto& [fieldValue, __] = fieldValueMeta;
        if (unlikely(isNull)) {
            continue;
        }

        T value;
        [[maybe_unused]] bool ret = autil::StringUtil::fromString(fieldValue, value);
        assert(ret);
        if (unlikely(!_hasValidData)) {
            _minValue.store(value);
            _maxValue.store(value);
            _hasValidData = true;
        } else {
            _minValue.store(std::min(_minValue.load(), value));
            _maxValue.store(std::max(_maxValue.load(), value));
        }
    }
    return Status::OK();
}

template <typename T>
Status MinMaxFieldMeta<T>::Store(autil::mem_pool::PoolBase* dumpPool,
                                 const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    indexlib::file_system::WriterOption option;
    const std::string& fileName = MIN_MAX_META_FILE_NAME;
    auto [status, fileWriter] = indexDirectory->CreateFileWriter(fileName, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "faile to create file writer for [%s], ErrorInfo: [%s]. ", fileName.c_str(),
                  status.ToString().c_str());
        return status;
    }
    std::string minMaxInfo = ToJsonString(*this);
    status = fileWriter->Write(minMaxInfo.c_str(), minMaxInfo.size()).Status();
    RETURN_IF_STATUS_ERROR(status, "minMax info writer failed to file [%s]", fileWriter->DebugString().c_str());
    status = fileWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "minMax writer [%s] close failed", fileWriter->DebugString().c_str());
    return Status::OK();
}

template <typename T>
void MinMaxFieldMeta<T>::GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                                   int64_t& dumpFileSize)
{
    currentMemUse = 0;
    dumpTmpMemUse = 0;
    dumpExpandMemUse = 0;
    dumpFileSize = 0;
    return;
}

} // namespace indexlib::index
