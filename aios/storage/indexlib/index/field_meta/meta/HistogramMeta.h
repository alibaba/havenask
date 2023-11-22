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
#include "indexlib/index/field_meta/meta/IFieldMeta.h"

namespace indexlib::index {

template <typename T, bool hasValueRange>
class HistogramMeta : public IFieldMeta
{
public:
    HistogramMeta(const FieldMetaConfig::FieldMetaInfo& info);
    ~HistogramMeta() {}

public:
    HistogramMeta<T, hasValueRange>* Clone() override;
    Status Build(const FieldValueBatch& fieldValues) override;
    Status Store(autil::mem_pool::PoolBase* dumpPool,
                 const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;

    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status Merge(const std::shared_ptr<IFieldMeta>& other) override;
    void GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                   int64_t& dumpFileSize) override;

public:
    size_t EstimateDocCountByValue(T fieldValue) const;
    // [from , to]
    size_t EstimateDocCountByRange(T from, T to) const;
    size_t GetNulls() const { return _nulls; }

private:
    void PreparePoints();

private:
    std::unordered_map<T, uint32_t> _fieldCounts;
    T _minValue = std::numeric_limits<T>::max();
    T _maxValue = std::numeric_limits<T>::min();
    size_t _nulls = 0;
    size_t _bins = 10000;
    // jsonize
    std::vector<T> _xPoints;
    std::vector<uint32_t> _yValues;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
