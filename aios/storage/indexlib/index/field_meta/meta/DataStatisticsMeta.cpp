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
#include "indexlib/index/field_meta/meta/DataStatisticsMeta.h"

#include "indexlib/util/KeyValueMap.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DataStatisticsMeta);

DataStatisticsMeta* DataStatisticsMeta::Clone()
{
    assert(false);
    return nullptr;
}
DataStatisticsMeta::DataStatisticsMeta(const FieldMetaConfig::FieldMetaInfo& info) : IFieldMeta(info)
{
    auto value = util::GetTypeValueFromJsonMap(info.metaParams, FieldMetaConfig::TOP_NUM, 1000);
    assert(value.has_value());
    _topNum = value.value();
}

Status DataStatisticsMeta::Build(const FieldValueBatch& fieldValues)
{
    for (const auto& [fieldValueMeta, _, __] : fieldValues) {
        _docCount++;
        const auto& [value, ___] = fieldValueMeta;
        _fieldCounts[value]++;
    }
    return Status::OK();
}

Status DataStatisticsMeta::Store(autil::mem_pool::PoolBase* dumpPool,
                                 const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    _ndv = _fieldCounts.size();

    GenerateLCVandMCV(_fieldCounts, _lcv, _mcv);

    indexlib::file_system::WriterOption option;
    const std::string& fileName = DATA_STATISTICS_META_FILE_NAME;
    auto [status, fileWriter] = indexDirectory->CreateFileWriter(fileName, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "faile to create file writer for [%s], ErrorInfo: [%s]. ", fileName.c_str(),
                  status.ToString().c_str());
        return status;
    }
    std::string fieldLenInfo = ToJsonString(*this);
    status = fileWriter->Write(fieldLenInfo.c_str(), fieldLenInfo.size()).Status();
    RETURN_IF_STATUS_ERROR(status, "data statistics info writer failed to file [%s]",
                           fileWriter->DebugString().c_str());
    status = fileWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "data statistics writer [%s] close failed", fileWriter->DebugString().c_str());
    return Status::OK();
}

Status DataStatisticsMeta::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    std::string fieldMeta;
    auto status = directory
                      ->Load(DATA_STATISTICS_META_FILE_NAME,
                             indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), fieldMeta)
                      .Status();
    RETURN_IF_STATUS_ERROR(status, "load data statistics meta from [%s] failed",
                           directory->GetPhysicalPath("").c_str());
    FromJsonString(*this, fieldMeta);
    return Status::OK();
}

void DataStatisticsMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("ndv", _ndv, _ndv);
    json.Jsonize("lcv", _lcv, _lcv);
    json.Jsonize("mcv", _mcv, _mcv);
    json.Jsonize("doc_count", _docCount, _docCount);
}

Status DataStatisticsMeta::Merge(const std::shared_ptr<IFieldMeta>& other)
{
    auto dataStatisticsMeta = std::dynamic_pointer_cast<DataStatisticsMeta>(other);
    if (!dataStatisticsMeta) {
        assert(false);
        return Status::Corruption("cast data statistics meta failed");
    }
    _ndv = std::max(_ndv, dataStatisticsMeta->_ndv);
    _docCount += dataStatisticsMeta->_docCount;

    std::unordered_map<std::string, uint32_t> fieldCounts;

    for (const auto& [key, count] : _lcv) {
        fieldCounts[key] += count;
    }
    for (const auto& [key, count] : _mcv) {
        if (_lcv.count(key)) {
            continue;
        }
        fieldCounts[key] += count;
    }
    for (const auto& [key, count] : dataStatisticsMeta->_lcv) {
        fieldCounts[key] += count;
    }
    for (const auto& [key, count] : dataStatisticsMeta->_mcv) {
        if (dataStatisticsMeta->_lcv.count(key)) {
            continue;
        }
        fieldCounts[key] += count;
    }
    _lcv.clear();
    _mcv.clear();
    GenerateLCVandMCV(fieldCounts, _lcv, _mcv);
    return Status::OK();
}

size_t DataStatisticsMeta::GetEstimateCount(const std::string& key) const
{
    if (_lcv.count(key)) {
        return _lcv.at(key);
    }
    if (_mcv.count(key)) {
        return _mcv.at(key);
    }
    if (_lcv.size() + _mcv.size() >= _ndv) {
        return 0;
    }

    size_t leftDocCount = _docCount;
    for (const auto& [key, count] : _lcv) {
        leftDocCount -= count;
    }
    for (const auto& [key, count] : _mcv) {
        leftDocCount -= count;
    }

    return leftDocCount / (_ndv - _lcv.size() - _mcv.size());
}

void DataStatisticsMeta::GenerateLCVandMCV(const std::unordered_map<std::string, uint32_t>& fieldCounts,
                                           std::map<std::string, uint32_t>& lcv, std::map<std::string, uint32_t>& mcv)
{
    std::priority_queue<CountAndKey, std::vector<CountAndKey>, std::greater<CountAndKey>> mcvHeap;
    std::priority_queue<CountAndKey> lcvHeap;

    for (const auto& [k, v] : fieldCounts) {
        mcvHeap.push({v, k});
        lcvHeap.push({v, k});
        while (mcvHeap.size() > _topNum) {
            mcvHeap.pop();
        }
        while (lcvHeap.size() > _topNum) {
            lcvHeap.pop();
        }
    }
    while (!mcvHeap.empty()) {
        auto [v, k] = mcvHeap.top();
        mcv[k] = v;
        mcvHeap.pop();
    }
    while (!lcvHeap.empty()) {
        auto [v, k] = lcvHeap.top();
        lcv[k] = v;
        lcvHeap.pop();
    }
}

void DataStatisticsMeta::GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                                   int64_t& dumpFileSize)
{
    currentMemUse = sizeof(_fieldCounts);
    dumpTmpMemUse = _ndv == 0 ? 0 : (_topNum * 1.0 / _ndv) * currentMemUse * 2;
    dumpExpandMemUse = 0;
    dumpFileSize = 0;
    return;
}

} // namespace indexlib::index
