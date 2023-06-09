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
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/util/MMapAllocator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, DocInfoAllocator);

const std::string DocInfoAllocator::DOCID_REFER_NAME = "docid";

DocInfoAllocator::DocInfoAllocator()
{
    _docInfoSize = 0;
    _allocator = std::make_unique<util::MMapAllocator>();
    _bufferPool = std::make_unique<autil::mem_pool::RecyclePool>(_allocator.get(), 10 * 1024 * 1024);
    CreateReference<docid_t>(DOCID_REFER_NAME, ft_int32, false);
}

DocInfoAllocator::~DocInfoAllocator()
{
    Release();
    for (auto it = _refers.begin(); it != _refers.end(); ++it) {
        delete it->second;
        it->second = NULL;
    }
    _refers.clear();
}

DocInfo* DocInfoAllocator::Allocate()
{
    // TODO: recycle pool can only allocate memory larger than 8 bytes.
    if (_docInfoSize < 8) {
        _docInfoSize = 8;
    }
    return (DocInfo*)_bufferPool->allocate(_docInfoSize);
}

void DocInfoAllocator::Deallocate(DocInfo* docInfo) { return _bufferPool->deallocate(docInfo, _docInfoSize); }

void DocInfoAllocator::Release()
{
    if (_bufferPool) {
        _bufferPool->release();
    }
}

} // namespace indexlib::index
