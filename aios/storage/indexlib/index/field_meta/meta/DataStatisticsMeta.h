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

#include <queue>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/field_format/field_meta/FieldMetaConvertor.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/meta/IFieldMeta.h"

namespace indexlib::index {

class DataStatisticsMeta : public IFieldMeta
{
public:
    DataStatisticsMeta(const FieldMetaConfig::FieldMetaInfo& info);
    ~DataStatisticsMeta() = default;

public:
    DataStatisticsMeta* Clone() override;
    Status Build(const FieldValueBatch& fieldValues) override;
    Status Store(autil::mem_pool::PoolBase* dumpPool,
                 const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;

    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status Merge(const std::shared_ptr<IFieldMeta>& other) override;
    void GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                   int64_t& dumpFileSize) override;

public:
    size_t GetEstimateCount(const std::string& key) const;
    size_t GetNDV() const { return _ndv; }
    std::map<std::string, uint32_t> GetLCV() const { return _lcv; }
    std::map<std::string, uint32_t> GetMCV() const { return _mcv; }

public:
    size_t TEST_GetDocCount() const { return _docCount; }

private:
    void GenerateLCVandMCV(const std::unordered_map<std::string, uint32_t>& fieldCounts,
                           std::map<std::string, uint32_t>& lcv, std::map<std::string, uint32_t>& mcv);

private:
    using CountAndKey = std::pair<size_t, std::string>;
    std::unordered_map<std::string, uint32_t> _fieldCounts;
    std::map<std::string, uint32_t> _lcv;
    std::map<std::string, uint32_t> _mcv;
    size_t _ndv = 0;
    size_t _topNum = 1000;
    size_t _docCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
