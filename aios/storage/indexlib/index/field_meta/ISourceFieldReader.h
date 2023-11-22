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
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlibv2::index {
class IIndexer;
}

namespace indexlib::index {
class FieldMetaConfig;

class ISourceFieldReader : private autil::NoCopyable
{
public:
    ISourceFieldReader() {}
    virtual ~ISourceFieldReader() {}

public:
    // for reader
    virtual Status Open(const std::shared_ptr<FieldMetaConfig>& indexConfig,
                        const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) = 0;
    virtual bool GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount) const = 0;

    // for merge
    virtual void PrepareReadContext() = 0;
    virtual bool GetFieldValue(int64_t key, autil::mem_pool::Pool* pool, std::string& field, bool& isNull) = 0;
    virtual std::shared_ptr<indexlibv2::index::IIndexer> GetSourceFieldDiskIndexer() const = 0;
    virtual FieldMetaConfig::MetaSourceType GetMetaStoreType() const = 0;

    virtual size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                   const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) const = 0;
    virtual size_t EvaluateCurrentMemUsed() const = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
