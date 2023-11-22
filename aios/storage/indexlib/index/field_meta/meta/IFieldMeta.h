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
#include "autil/legacy/jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlib::index {
struct FieldValueMeta;

class IFieldMeta : public autil::legacy::Jsonizable
{
public:
    IFieldMeta() {}
    IFieldMeta(const FieldMetaConfig::FieldMetaInfo& fieldMetaInfo) : _fieldMetaInfo(fieldMetaInfo) {}
    virtual ~IFieldMeta() {}

public:
    using FieldValueBatch = std::vector<std::tuple<FieldValueMeta, bool, docid_t>>;

public:
    virtual IFieldMeta* Clone() = 0;
    virtual Status Build(const FieldValueBatch& fieldValues) = 0;
    virtual Status Store(autil::mem_pool::PoolBase* dumpPool,
                         const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) = 0;

    virtual Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory) = 0;
    virtual Status Merge(const std::shared_ptr<IFieldMeta>& other) = 0;
    virtual void GetMemUse(int64_t& currentMemUse, int64_t& dumpTmpMemUse, int64_t& dumpExpandMemUse,
                           int64_t& dumpFileSize) = 0;
    std::string GetFieldMetaName() const { return _fieldMetaInfo.metaName; }

protected:
    FieldMetaConfig::FieldMetaInfo _fieldMetaInfo;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
