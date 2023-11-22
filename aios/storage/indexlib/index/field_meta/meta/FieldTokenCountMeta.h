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
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"
#include "indexlib/index/field_meta/meta/IFieldMeta.h"

namespace indexlib::index {

class FieldTokenCountMeta : public IFieldMeta
{
public:
    FieldTokenCountMeta(const FieldMetaConfig::FieldMetaInfo& info) : IFieldMeta(info) {}
    ~FieldTokenCountMeta() = default;

    FieldTokenCountMeta(const FieldTokenCountMeta& other)
        : _docCount(other._docCount)
        , _totalTokenCount(other._totalTokenCount)
        , _avgFieldTokenCount(other._avgFieldTokenCount.load())
    {
    }

public:
    FieldTokenCountMeta* Clone() override;
    Status Build(const FieldValueBatch& fieldValues) override;
    Status Store(autil::mem_pool::PoolBase* dumpPool,
                 const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory) override;
    Status Merge(const std::shared_ptr<IFieldMeta>& other) override;
    void GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                   int64_t& dumpFileSize) override;

    double GetAvgFieldTokenCount() const;

private:
    size_t _docCount = 0;
    size_t _totalTokenCount = 0;
    std::atomic<double> _avgFieldTokenCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
