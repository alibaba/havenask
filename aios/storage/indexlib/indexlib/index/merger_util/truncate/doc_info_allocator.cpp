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
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"

#include "indexlib/util/MMapAllocator.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::util;
namespace indexlib::index::legacy {
IE_LOG_SETUP(index, DocInfoAllocator);

const std::string DocInfoAllocator::DOCID_REFER_NAME = "docid";

DocInfoAllocator::DocInfoAllocator()
{
    mDocInfoSize = 0;
    mAllocator = new MMapAllocator();
    mBufferPool = new RecyclePool(mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    CreateReference<docid_t>(DOCID_REFER_NAME, ft_int32, false);
}

DocInfoAllocator::~DocInfoAllocator()
{
    Release();

    delete mBufferPool;
    delete mAllocator;
    for (ReferenceMap::iterator it = mRefers.begin(); it != mRefers.end(); ++it) {
        delete it->second;
        it->second = NULL;
    }
    mRefers.clear();
}

DocInfo* DocInfoAllocator::Allocate()
{
    // TODO: recycle pool can only allocate memory larger than 8 bytes.
    if (mDocInfoSize < 8) {
        mDocInfoSize = 8;
    }
    return (DocInfo*)mBufferPool->allocate(mDocInfoSize);
}

void DocInfoAllocator::Deallocate(DocInfo* docInfo) { return mBufferPool->deallocate(docInfo, mDocInfoSize); }

void DocInfoAllocator::Release()
{
    if (mBufferPool) {
        mBufferPool->release();
    }
}
} // namespace indexlib::index::legacy
