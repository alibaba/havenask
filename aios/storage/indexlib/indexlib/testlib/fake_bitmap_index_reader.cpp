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
#include "indexlib/testlib/fake_bitmap_index_reader.h"

#include "indexlib/testlib/fake_bitmap_posting_iterator.h"
#include "indexlib/testlib/fake_posting_maker.h"

using namespace std;
using namespace indexlib::index;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeBitmapIndexReader);

FakeBitmapIndexReader::FakeBitmapIndexReader(const string& datas)
{
    FakePostingMaker::makeFakePostingsDetail(datas, _map);
}

FakeBitmapIndexReader::~FakeBitmapIndexReader() {}

index::Result<PostingIterator*> FakeBitmapIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                              PostingType type, autil::mem_pool::Pool* sessionPool)
{
    FakePostingIterator::Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    return POOL_COMPATIBLE_NEW_CLASS(sessionPool, FakeBitmapPostingIterator, it->second, statePoolSize, sessionPool);
}
}} // namespace indexlib::testlib
