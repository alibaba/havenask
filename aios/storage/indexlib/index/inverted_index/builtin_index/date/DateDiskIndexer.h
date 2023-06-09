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
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"

namespace indexlib::index {
class DateLeafReader;

class DateDiskIndexer : public InvertedDiskIndexer
{
public:
    explicit DateDiskIndexer(const indexlibv2::index::IndexerParameter& indexerParam);
    ~DateDiskIndexer();
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    std::shared_ptr<InvertedLeafReader> GetReader() const override;

private:
    std::shared_ptr<DateLeafReader> _dateLeafReader;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
