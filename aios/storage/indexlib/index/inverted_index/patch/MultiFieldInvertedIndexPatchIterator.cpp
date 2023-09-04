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
#include "indexlib/index/inverted_index/patch/MultiFieldInvertedIndexPatchIterator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/patch/SingleFieldInvertedIndexPatchIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, MultiFieldInvertedIndexPatchIterator);

MultiFieldInvertedIndexPatchIterator::MultiFieldInvertedIndexPatchIterator(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, bool isSub,
    const std::shared_ptr<indexlib::file_system::IDirectory>& patchExtraDir)
    : _schema(schema)
    , _isSub(isSub)
    , _cursor(-1)
    , _patchLoadExpandSize(0)
    , _patchExtraDir(patchExtraDir)
{
}

MultiFieldInvertedIndexPatchIterator::~MultiFieldInvertedIndexPatchIterator() {}

Status MultiFieldInvertedIndexPatchIterator::AddSingleFieldPatchIterator(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedIndexConfig,
    const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments)
{
    std::unique_ptr<SingleFieldInvertedIndexPatchIterator> singleFieldPatchIter(
        new SingleFieldInvertedIndexPatchIterator(invertedIndexConfig, _patchExtraDir));
    auto status = singleFieldPatchIter->Init(segments);
    RETURN_IF_STATUS_ERROR(status, "add single field patch iterator for index [%s] failed",
                           invertedIndexConfig->GetIndexName().c_str());
    _patchLoadExpandSize += singleFieldPatchIter->GetPatchLoadExpandSize();
    _singleFieldPatchIters.push_back(std::move(singleFieldPatchIter));
    return Status::OK();
}

Status
MultiFieldInvertedIndexPatchIterator::Init(const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments)
{
    for (auto indexConfig : _schema->GetIndexConfigs(INVERTED_INDEX_TYPE_STR)) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig);
        if (!invertedIndexConfig->IsIndexUpdatable()) {
            continue;
        }
        if (invertedIndexConfig->GetShardingType() ==
            indexlibv2::config::InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING) {
            auto shardingIndexConfigs = invertedIndexConfig->GetShardingIndexConfigs();
            for (const auto& shardingIndexConfig : shardingIndexConfigs) {
                RETURN_IF_STATUS_ERROR(AddSingleFieldPatchIterator(shardingIndexConfig, segments),
                                       "add sharding patch iterator failed");
            }
        }
        RETURN_IF_STATUS_ERROR(AddSingleFieldPatchIterator(invertedIndexConfig, segments), "add patch iterator failed");
    }
    if (!_singleFieldPatchIters.empty()) {
        _cursor = 0;
        Log();
    }
    AUTIL_LOG(INFO, "find [%lu] index patches", _singleFieldPatchIters.size());
    return Status::OK();
}

void MultiFieldInvertedIndexPatchIterator::Log() const
{
    if (_cursor < _singleFieldPatchIters.size()) {
        AUTIL_LOG(INFO, "next index patch field [%s], patch item[%lu]",
                  _singleFieldPatchIters[_cursor]->GetIndexName().c_str(),
                  _singleFieldPatchIters[_cursor]->GetPatchItemCount());
    }
}

std::unique_ptr<IndexUpdateTermIterator> MultiFieldInvertedIndexPatchIterator::Next()
{
    if (_cursor < 0) {
        return nullptr;
    }
    while (_cursor < _singleFieldPatchIters.size()) {
        auto& fieldIter = _singleFieldPatchIters[_cursor];
        std::unique_ptr<IndexUpdateTermIterator> r = fieldIter->Next();
        if (r) {
            r->SetIsSub(_isSub);
            return r;
        } else {
            ++_cursor;
            Log();
        }
    }
    return nullptr;
}

// void MultiFieldInvertedIndexPatchIterator::CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems)
// {
//     if (_cursor < 0) {
//         return;
//     }
//     for (; _cursor < _singleFieldPatchIters.size(); ++_cursor) {
//         _singleFieldPatchIters[_cursor]->CreateIndependentPatchWorkItems(workItems);
//     }
// }

} // namespace indexlib::index
