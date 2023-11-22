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
#include "indexlib/index/attribute/AttributeIndexFactory.h"

namespace indexlib::index {

class SectionAttributeIndexFactory : public indexlibv2::index::AttributeIndexFactory
{
public:
    SectionAttributeIndexFactory();
    ~SectionAttributeIndexFactory();

public:
    std::shared_ptr<indexlibv2::index::IDiskIndexer>
    CreateDiskIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                      const indexlibv2::index::DiskIndexerParameter& indexerParam) const override;
    std::unique_ptr<indexlibv2::index::IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) const override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
