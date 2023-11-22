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
#include "autil/Span.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/IDiskIndexer.h"

namespace indexlibv2::table {

class VirtualAttributeDiskIndexer : public index::IDiskIndexer
{
public:
    VirtualAttributeDiskIndexer(const std::shared_ptr<index::IDiskIndexer>& attrDiskIndexer);
    ~VirtualAttributeDiskIndexer();
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;
    std::shared_ptr<index::IDiskIndexer> GetDiskIndexer() const;

public:
    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey);

private:
    std::shared_ptr<index::IDiskIndexer> _impl;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
