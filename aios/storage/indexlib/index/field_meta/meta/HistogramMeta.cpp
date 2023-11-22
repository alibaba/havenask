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
#include "indexlib/index/field_meta/meta/HistogramMeta.h"

#include "indexlib/util/KeyValueMap.h"

namespace indexlib::index {

AUTIL_LOG_SETUP_TEMPLATE_WITH_TYPENAME2(indexlib.index, HistogramMeta, typename, T, bool, hasValueRange);
template <typename T, bool hasValueRange>
HistogramMeta<T, hasValueRange>::HistogramMeta(const FieldMetaConfig::FieldMetaInfo& info) : IFieldMeta(info)
{
    auto binsValue = util::GetTypeValueFromJsonMap(info.metaParams, FieldMetaConfig::BINS, 10000u);
    assert(binsValue.has_value());
    _bins = binsValue.value();

    if constexpr (hasValueRange) {
        _maxValue = util::GetTypeValueFromJsonMap(info.metaParams, FieldMetaConfig::MAX_VALUE, 0u).value();
        _minValue = util::GetTypeValueFromJsonMap(info.metaParams, FieldMetaConfig::MIN_VALUE, 0u).value();
        PreparePoints();
    } else {
        _xPoints.resize(1, 0);
        _yValues.resize(2, 0);
    }
}

template <typename T, bool hasValueRange>
HistogramMeta<T, hasValueRange>* HistogramMeta<T, hasValueRange>::Clone()
{
    assert(false);
    return nullptr;
}

template <typename T, bool hasValueRange>
Status HistogramMeta<T, hasValueRange>::Build(const FieldValueBatch& fieldValues)
{
    for (const auto& [fieldValueMeta, isNull, _] : fieldValues) {
        const auto& [fieldValue, __] = fieldValueMeta;
        if (unlikely(isNull)) {
            _nulls++;
            continue;
        }
        T value;
        [[maybe_unused]] bool ret = autil::StringUtil::fromString(fieldValue, value);
        assert(ret);
        _minValue = std::min(_minValue, value);
        _maxValue = std::max(_maxValue, value);
        if constexpr (!hasValueRange) {
            _fieldCounts[value]++;
        } else {
            assert(!_yValues.empty());
            auto iter = std::upper_bound(_xPoints.begin(), _xPoints.end(), value);
            if (iter == _xPoints.end()) {
                _yValues[_yValues.size() - 1]++;
            } else {
                size_t index = iter - _xPoints.begin();
                _yValues[index]++;
            }
        }
    }
    return Status::OK();
}

template <typename T, bool hasValueRange>
Status HistogramMeta<T, hasValueRange>::Store(autil::mem_pool::PoolBase* dumpPool,
                                              const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    if constexpr (!hasValueRange) {
        // no data
        PreparePoints();
        for (const auto& [key, count] : _fieldCounts) {
            assert(!_yValues.empty());
            auto iter = std::upper_bound(_xPoints.begin(), _xPoints.end(), key);
            if (iter == _xPoints.end()) {
                _yValues[_yValues.size() - 1] += count;
            } else {
                size_t index = iter - _xPoints.begin();
                _yValues[index] += count;
            }
        }
    }

    indexlib::file_system::WriterOption option;
    const std::string& fileName = HISTOGRAM_META_FILE_NAME;
    auto [status, fileWriter] = indexDirectory->CreateFileWriter(fileName, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "faile to create file writer for [%s], ErrorInfo: [%s]. ", fileName.c_str(),
                  status.ToString().c_str());
        return status;
    }
    std::string fieldLenInfo = ToJsonString(*this);
    status = fileWriter->Write(fieldLenInfo.c_str(), fieldLenInfo.size()).Status();
    RETURN_IF_STATUS_ERROR(status, "histogram info writer failed to file [%s]", fileWriter->DebugString().c_str());
    status = fileWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "histogram writer [%s] close failed", fileWriter->DebugString().c_str());
    return Status::OK();
}

template <typename T, bool hasValueRange>
Status HistogramMeta<T, hasValueRange>::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    std::string fieldMeta;
    auto status = directory
                      ->Load(HISTOGRAM_META_FILE_NAME,
                             indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), fieldMeta)
                      .Status();
    RETURN_IF_STATUS_ERROR(status, "load histogram meta from [%s] failed", directory->GetPhysicalPath("").c_str());
    FromJsonString(*this, fieldMeta);
    return Status::OK();
}

template <typename T, bool hasValueRange>
void HistogramMeta<T, hasValueRange>::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("values", _xPoints, _xPoints);
    json.Jsonize("counts", _yValues, _yValues);
    json.Jsonize("nulls", _nulls, _nulls);
    json.Jsonize("max_value", _maxValue, _maxValue);
    json.Jsonize("min_value", _minValue, _minValue);
}

template <typename T, bool hasValueRange>
Status HistogramMeta<T, hasValueRange>::Merge(const std::shared_ptr<IFieldMeta>& other)
{
    auto histogramMeta = std::dynamic_pointer_cast<HistogramMeta<T, hasValueRange>>(other);
    if (!histogramMeta) {
        assert(false);
        return Status::Corruption("cast histogram meta meta failed");
    }

    AUTIL_LOG(DEBUG, "[%s] merge with [%s]", ToJsonString(*this, true).c_str(),
              ToJsonString(*histogramMeta, true).c_str());
    T originMaxValue = _maxValue;
    _minValue = std::min(_minValue, histogramMeta->_minValue);
    _maxValue = std::max(_maxValue, histogramMeta->_maxValue);
    _nulls = _nulls + histogramMeta->_nulls;
    if constexpr (hasValueRange) {
        assert(_xPoints.size() == histogramMeta->_xPoints.size());
        assert(_yValues.size() == histogramMeta->_yValues.size());
        for (size_t i = 0; i < _yValues.size(); i++) {
            _yValues[i] += histogramMeta->_yValues[i];
        }
        AUTIL_LOG(INFO, "merge final result [%s]", ToJsonString(*this, true).c_str());
        return Status::OK();
    }
    assert(!_xPoints.empty() && !histogramMeta->_xPoints.empty());
    auto originXPoints = _xPoints;
    auto originYValues = _yValues;
    PreparePoints();

    size_t begin1 = 0, begin2 = 0;
    for (size_t i = 0; i < _xPoints.size() - 1; ++i) {
        auto [begin, end] = std::make_pair(_xPoints[i], _xPoints[i + 1]);
        while (begin1 < originXPoints.size() - 1) {
            auto [originBegin, originEnd] = std::make_pair(originXPoints[begin1], originXPoints[begin1 + 1]);
            if (originEnd <= begin) {
                begin1++;
            } else if (originBegin >= end) {
                break;
            } else {
                double ratio =
                    1.0 * (std::min(end, originEnd) - std::max(originBegin, begin)) / (originEnd - originBegin);
                _yValues[i + 1] += std::ceil(ratio * originYValues[begin1 + 1]);
                if (originEnd < end) {
                    begin1++;
                } else {
                    break;
                }
            }
        }
        while (begin2 < histogramMeta->_xPoints.size() - 1) {
            auto [otherBegin, otherEnd] =
                std::make_pair(histogramMeta->_xPoints[begin2], histogramMeta->_xPoints[begin2 + 1]);
            if (otherEnd <= begin) {
                begin2++;
            } else if (otherBegin >= end) {
                break;
            } else {
                double ratio = 1.0 * (std::min(end, otherEnd) - std::max(otherBegin, begin)) / (otherEnd - otherBegin);
                _yValues[i + 1] += std::ceil(ratio * histogramMeta->_yValues[begin2 + 1]);
                if (otherEnd < end) {
                    begin2++;
                } else {
                    break;
                }
            }
        }
    }
    while (begin1 < originXPoints.size() - 1) {
        auto [originBegin, originEnd] = std::make_pair(originXPoints[begin1], originXPoints[begin1 + 1]);
        double ratio = 1.0 * (originEnd - std::max(originBegin, _xPoints.back())) / (originEnd - originBegin);
        _yValues[_yValues.size() - 1] += std::ceil(ratio * originYValues[begin1 + 1]);
        begin1++;
    }

    while (begin2 < histogramMeta->_xPoints.size() - 1) {
        auto [otherBegin, otherEnd] =
            std::make_pair(histogramMeta->_xPoints[begin2], histogramMeta->_xPoints[begin2 + 1]);
        double ratio = 1.0 * (otherEnd - std::max(otherBegin, _xPoints.back())) / (otherEnd - otherBegin);
        _yValues[_yValues.size() - 1] += std::ceil(ratio * histogramMeta->_yValues[begin2 + 1]);
        begin2++;
    }

    //因为存储结构为[), 只有最大值是std::numeric_limits<T>::max()情况下，最后一个range会有doc,需要加一下
    if (originMaxValue == std::numeric_limits<T>::max()) {
        _yValues[_xPoints.size()] += originYValues.back();
    }

    if (histogramMeta->_maxValue == std::numeric_limits<T>::max()) {
        _yValues[_xPoints.size()] += histogramMeta->_yValues.back();
    }

    AUTIL_LOG(DEBUG, "merge final result [%s]", ToJsonString(*this, true).c_str());
    return Status::OK();
}

template <typename T, bool hasValueRange>
size_t HistogramMeta<T, hasValueRange>::EstimateDocCountByRange(T from, T to) const
{
    if (from > to) {
        AUTIL_LOG(WARN, "invalid query as from [%ld] is bigger than to [%ld]", from, to);
        return 0;
    }
    size_t ret = 0;
    // trans to [from, to)
    if (to == std::numeric_limits<T>::max()) {
        ret = EstimateDocCountByValue(to);
    } else {
        to++;
    }

    if (from < _xPoints.front()) {
        double ratio = 1.0 * (size_t)(std::min(_xPoints.front(), to) - from) /
                       (size_t)(_xPoints.front() - std::numeric_limits<T>::min());
        ret += std::ceil(ratio * _yValues[0]);
    }
    for (size_t i = 0; i < _xPoints.size() - 1; ++i) {
        auto [begin, end] = std::make_pair(_xPoints[i], _xPoints[i + 1]);
        if (end <= from) {
            continue;
        } else if (begin >= to) {
            return ret;
        } else {
            double ratio = 1.0 * (std::min(to, end) - std::max(from, begin)) / (end - begin);
            ret += std::ceil(ratio * _yValues[i + 1]);
        }
    }
    if (_xPoints.back() >= to) {
        return ret;
    }
    double ratio = 1.0 * (size_t)(to - std::max(from, _xPoints.back())) /
                   (size_t)(std::numeric_limits<T>::max() - _xPoints.back());

    ret += std::ceil(ratio * _yValues.back());
    return ret;
}

template <typename T, bool hasValueRange>
size_t HistogramMeta<T, hasValueRange>::EstimateDocCountByValue(T fieldValue) const
{
    auto iter = std::upper_bound(_xPoints.begin(), _xPoints.end(), fieldValue);
    size_t index = iter - _xPoints.begin();
    size_t width = 0;
    if (unlikely(iter == _xPoints.end())) {
        width = (size_t)std::numeric_limits<T>::max() - (size_t)_xPoints.back();
        if (width != std::numeric_limits<size_t>::max()) {
            width++;
        }
    } else if (unlikely(index == 0)) {
        width = (size_t)_xPoints.front() - (size_t)std::numeric_limits<T>::min();
    } else {
        width = _xPoints[index] - _xPoints[index - 1];
    }
    return _yValues[index] / width;
}

template <typename T, bool hasValueRange>
void HistogramMeta<T, hasValueRange>::GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse,
                                                int64_t& dumpExpandMemUse, int64_t& dumpFileSize)
{
    currentMemUse = sizeof(_fieldCounts);
    dumpTmpMemUse = 0;
    dumpExpandMemUse = 0;
    dumpFileSize = 0;
}

template <typename T, bool hasValueRange>
void HistogramMeta<T, hasValueRange>::PreparePoints()
{
    // no data
    if (_minValue > _maxValue) {
        _xPoints.resize(1, 0);
        _yValues.resize(2, 0);
        return;
    }
    size_t valueRangeLen = size_t(_maxValue) - size_t(_minValue);
    if (valueRangeLen != std::numeric_limits<size_t>::max()) {
        valueRangeLen++;
    }
    size_t bins = std::min(valueRangeLen, _bins);

    T width = valueRangeLen / bins;
    assert(width >= 1);
    _xPoints.clear();
    _xPoints.reserve(bins + 1);
    T curValue = _minValue;
    for (int i = 0; i < bins; i++) {
        _xPoints.push_back(curValue);
        curValue += width;
    }
    if (_maxValue != std::numeric_limits<T>::max()) {
        _xPoints.push_back(_maxValue + 1);
    } else if (_xPoints.back() != std::numeric_limits<T>::max()) {
        _xPoints.push_back(_maxValue);
    }
    _yValues.clear();
    _yValues.resize(_xPoints.size() + 1, 0);
}

template class HistogramMeta<int64_t, true>;
template class HistogramMeta<uint64_t, true>;
template class HistogramMeta<int64_t, false>;
template class HistogramMeta<uint64_t, false>;

} // namespace indexlib::index
