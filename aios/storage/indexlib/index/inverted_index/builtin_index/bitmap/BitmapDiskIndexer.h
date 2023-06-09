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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/IDiskIndexer.h"

namespace indexlib::index {
class BitmapLeafReader;
class DictKeyInfo;

class BitmapDiskIndexer : public indexlibv2::index::IDiskIndexer
{
public:
    explicit BitmapDiskIndexer(uint64_t docCount);
    ~BitmapDiskIndexer() = default;

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    void Update(docid_t docId, const DictKeyInfo& key, bool isDelete);

    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;
    std::shared_ptr<BitmapLeafReader> GetReader() const { return _leafReader; }

private:
    std::shared_ptr<BitmapLeafReader> _leafReader;
    uint64_t _docCount = 0;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
