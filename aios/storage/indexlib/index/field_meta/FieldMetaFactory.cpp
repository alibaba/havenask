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
#include "indexlib/index/field_meta/FieldMetaFactory.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/FieldMetaDiskIndexer.h"
#include "indexlib/index/field_meta/FieldMetaMemIndexer.h"
#include "indexlib/index/field_meta/FieldMetaMerger.h"
#include "indexlib/index/field_meta/FieldMetaReader.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaFactory);

std::shared_ptr<indexlibv2::index::IDiskIndexer>
FieldMetaFactory::CreateDiskIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                    const indexlibv2::index::DiskIndexerParameter& indexerParam) const
{
    auto fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    if (!fieldMetaConfig) {
        AUTIL_LOG(ERROR, "expect fieldMetaConfig, actual: %s", indexConfig->GetIndexType().c_str());
        return nullptr;
    }
    return std::make_shared<FieldMetaDiskIndexer>(indexerParam);
}

std::shared_ptr<indexlibv2::index::IMemIndexer>
FieldMetaFactory::CreateMemIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                   const indexlibv2::index::MemIndexerParameter&) const
{
    auto fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    if (!fieldMetaConfig) {
        AUTIL_LOG(ERROR, "expect fieldMetaConfig, actual: %s", indexConfig->GetIndexType().c_str());
        return nullptr;
    }
    return std::make_shared<FieldMetaMemIndexer>();
}

std::unique_ptr<indexlibv2::index::IIndexReader>
FieldMetaFactory::CreateIndexReader(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                    const indexlibv2::index::IndexReaderParameter&) const
{
    return std::make_unique<FieldMetaReader>();
}

std::unique_ptr<indexlibv2::index::IIndexMerger>
FieldMetaFactory::CreateIndexMerger(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<FieldMetaMerger>();
}

std::unique_ptr<indexlibv2::config::IIndexConfig>
FieldMetaFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<FieldMetaConfig>();
}

std::string FieldMetaFactory::GetIndexPath() const { return FIELD_META_INDEX_PATH; }

REGISTER_INDEX_FACTORY(field_meta, FieldMetaFactory);
} // namespace indexlib::index
