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
#include "indexlib/testlib/fake_text_index_reader.h"

#include "indexlib/testlib/fake_posting_maker.h"

using namespace std;
using namespace indexlib::index;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeTextIndexReader);

FakeTextIndexReader::FakeTextIndexReader(const string& mpStr) { FakePostingMaker::makeFakePostingsDetail(mpStr, _map); }

FakeTextIndexReader::FakeTextIndexReader(const string& mpStr, const FakePostingIterator::PositionMap& posMap)
{
    FakePostingMaker::makeFakePostingsDetail(mpStr, _map);
    _posMap = posMap;
}

index::Result<PostingIterator*> FakeTextIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                            PostingType type, autil::mem_pool::Pool* sessionPool)
{
    FakePostingIterator::Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    FakePostingIterator* fakeIterator =
        POOL_COMPATIBLE_NEW_CLASS(sessionPool, FakePostingIterator, it->second, statePoolSize, sessionPool);
    FakePostingIterator::IteratorTypeMap::iterator typeIt = _typeMap.find(term.GetWord());
    if (typeIt != _typeMap.end()) {
        fakeIterator->setType(typeIt->second);
    }
    FakePostingIterator::PositionMap::iterator posIt = _posMap.find(term.GetWord());
    if (posIt != _posMap.end()) {
        fakeIterator->setPosition(posIt->second);
    }
    return fakeIterator;
}

const SectionAttributeReader* FakeTextIndexReader::GetSectionReader(const std::string& indexName) const
{
    _ptr.reset(new FakeSectionAttributeReader(_docSectionMap));
    return _ptr.get();
}
}} // namespace indexlib::testlib
