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
#include "indexlib/index/inverted_index/patch/IInvertedIndexPatchIterator.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}
namespace indexlibv2::index {
class PatchFileInfos;
}
namespace indexlibv2::framework {
class Segment;
}
namespace indexlib::index {
class SingleFieldIndexSegmentPatchIterator;
}
namespace indexlib::file_system {
class IDirectory;
}
namespace indexlib::index {
class SingleFieldInvertedIndexPatchIterator : public IInvertedIndexPatchIterator
{
public:
    SingleFieldInvertedIndexPatchIterator(
        const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedIndexConfig,
        const std::shared_ptr<indexlib::file_system::IDirectory>& patchExtraDir);
    ~SingleFieldInvertedIndexPatchIterator() {};

public:
    Status Init(const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments);
    size_t GetPatchItemCount() const { return _patchItemCount; }
    const std::string& GetIndexName() const;

public:
    std::unique_ptr<IndexUpdateTermIterator> Next() override;
    // void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;
    size_t GetPatchLoadExpandSize() const override { return _patchLoadExpandSize; }

private:
    void Log() const;

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _invertedIndexConfig;
    std::vector<std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>> _segmentIterators;
    std::shared_ptr<indexlib::file_system::IDirectory> _patchExtraDir;
    int64_t _cursor;
    size_t _patchLoadExpandSize;
    size_t _patchItemCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
