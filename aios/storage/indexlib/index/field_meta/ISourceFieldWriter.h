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

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlibv2::framework {
struct DumpParams;
}

namespace indexlib::index {
struct FieldValueMeta;
class FieldMetaConfig;

class ISourceFieldWriter : private autil::NoCopyable
{
public:
    ISourceFieldWriter() {}
    virtual ~ISourceFieldWriter() {}

public:
    using FieldValue = std::tuple<FieldValueMeta, bool, docid_t>;
    using FieldValueBatch = std::vector<FieldValue>;

public:
    virtual Status Init(const std::shared_ptr<FieldMetaConfig>& config) = 0;
    virtual Status Build(const FieldValueBatch& docBatch) = 0;
    virtual Status Dump(autil::mem_pool::PoolBase* dumpPool,
                        const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory,
                        const std::shared_ptr<indexlibv2::framework::DumpParams>& params) = 0;
    virtual bool GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount) = 0;
    virtual void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater) = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
