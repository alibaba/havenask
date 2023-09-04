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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/primary_key/PrimaryKeyWriter.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

namespace indexlib::index {
class ISinglePrimaryKeyBuilder : public autil::NoCopyable
{
public:
    virtual ~ISinglePrimaryKeyBuilder() {}

public:
    virtual Status AddDocument(indexlibv2::document::IDocument* doc) = 0;
    virtual Status Init(const indexlibv2::framework::TabletData& tabletData,
                        const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) = 0;
};

template <typename Key>
class SinglePrimaryKeyBuilder : public ISinglePrimaryKeyBuilder
{
public:
    SinglePrimaryKeyBuilder();
    ~SinglePrimaryKeyBuilder();

public:
    Status AddDocument(indexlibv2::document::IDocument* doc) override;

public:
    Status Init(const indexlibv2::framework::TabletData& tabletData,
                const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) override;

private:
    std::shared_ptr<indexlibv2::index::PrimaryKeyWriter<Key>> _primaryKeyWriter = nullptr;

    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SinglePrimaryKeyBuilder, T);

template <typename Key>
SinglePrimaryKeyBuilder<Key>::SinglePrimaryKeyBuilder()
{
}

template <typename Key>
SinglePrimaryKeyBuilder<Key>::~SinglePrimaryKeyBuilder()
{
}

template <typename Key>
Status SinglePrimaryKeyBuilder<Key>::AddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    return _primaryKeyWriter->AddDocument(doc);
}

template <typename Key>
Status SinglePrimaryKeyBuilder<Key>::Init(const indexlibv2::framework::TabletData& tabletData,
                                          const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    auto primaryKeyIndexConfig = std::static_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);
    assert(primaryKeyIndexConfig != nullptr);

    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        indexlibv2::framework::Segment* segment = it->get();
        if (segment->GetSegmentStatus() != indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
            continue;
        }
        const std::string& indexName = primaryKeyIndexConfig->GetIndexName();
        auto [status, indexer] = segment->GetIndexer(primaryKeyIndexConfig->GetIndexType(), indexName);
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        assert(indexer != nullptr);
        _primaryKeyWriter = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyWriter<Key>>(indexer);
        assert(_primaryKeyWriter != nullptr);
    }
    if (_primaryKeyWriter == nullptr) {
        return Status::InternalError("No PrimaryKeyWriter intialized.");
    }
    return Status::OK();
}

} // namespace indexlib::index
