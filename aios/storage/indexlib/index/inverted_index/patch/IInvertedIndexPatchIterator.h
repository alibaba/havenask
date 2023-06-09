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

namespace indexlib::index {
class PatchWorkItem;
class IndexUpdateTermIterator;
} // namespace indexlib::index

namespace indexlib::index {
class IInvertedIndexPatchIterator : private autil::NoCopyable
{
public:
    IInvertedIndexPatchIterator() = default;
    virtual ~IInvertedIndexPatchIterator() = default;

public:
    virtual std::unique_ptr<IndexUpdateTermIterator> Next() = 0;
    // TODO: 参考正排，等待支持多线程LoadPatch
    // virtual void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) = 0;
    virtual size_t GetPatchLoadExpandSize() const = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
