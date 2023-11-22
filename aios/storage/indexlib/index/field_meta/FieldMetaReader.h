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
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/field_meta/FieldMetaDiskIndexer.h"
#include "indexlib/index/field_meta/FieldMetaMemIndexer.h"
#include "indexlib/index/field_meta/meta/IFieldMeta.h"

namespace indexlib::index {

class FieldMetaReader : public indexlibv2::index::IIndexReader
{
public:
    FieldMetaReader();
    ~FieldMetaReader();

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override;

public:
    std::shared_ptr<IFieldMeta> GetEstimateTableFieldMeta(const std::string& fieldMetaType) const;
    std::shared_ptr<IFieldMeta> GetTableFieldMeta(const std::string& fieldMetaType) const;
    std::shared_ptr<IFieldMeta> GetSegmentFieldMeta(const std::string& fieldMetaType,
                                                    const DocIdRange& docidRange) const;
    bool GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount);

private:
    std::string TEST_GetSourceField(int64_t key);

private:
    Status InitTableFieldMetas(const std::shared_ptr<FieldMetaConfig>& fieldMetaConfig);
    std::pair<Status, std::shared_ptr<IFieldMeta>>
    UpdateTableFieldMetas(const std::string& fieldMetaType, const std::shared_ptr<IFieldMeta>& fieldMeta) const;
    std::shared_ptr<IFieldMeta> GetFieldMeta(const std::string& metaName) const;

private:
    std::unordered_map<std::string, std::shared_ptr<IFieldMeta>> _tableFieldMetas;
    std::vector<std::pair<DocIdRange, std::shared_ptr<FieldMetaDiskIndexer>>> _diskIndexers;
    std::vector<std::pair<docid_t, std::shared_ptr<FieldMetaMemIndexer>>> _memIndexers;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
