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

#include <memory>
#include <queue>

#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/IndexPatchFileReader.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/util/Exception.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlib::index {
class SingleTermIndexSegmentPatchIterator;
}

namespace indexlib::index {
class SingleFieldIndexSegmentPatchIterator
{
public:
    SingleFieldIndexSegmentPatchIterator(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                         segmentid_t targetSegmentId)
        : _indexConfig(indexConfig)
        , _targetSegmentId(targetSegmentId)
        , _initialState(true)
        , _patchLoadExpandSize(0)
        , _patchItemCount(0)
    {
    }
    virtual ~SingleFieldIndexSegmentPatchIterator();

public:
    Status AddPatchFile(const std::shared_ptr<file_system::IDirectory>& directory, const std::string& fileName,
                        segmentid_t srcSegmentId, segmentid_t targetSegmentId);
    // virtual for test
    virtual std::unique_ptr<SingleTermIndexSegmentPatchIterator> NextTerm();
    bool Empty() const;
    segmentid_t GetTargetSegmentId() const { return _targetSegmentId; }
    size_t GetPatchItemCount() const { return _patchItemCount; }
    size_t GetPatchLoadExpandSize() const { return _patchLoadExpandSize; }

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    segmentid_t _targetSegmentId;
    IndexPatchHeap _patchHeap;
    indexlib::index::DictKeyInfo _currentTermKey;
    bool _initialState;
    size_t _patchLoadExpandSize;
    size_t _patchItemCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
