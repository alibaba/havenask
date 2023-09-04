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

#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"

namespace indexlib { namespace index {

class MockPrimaryKeyIndexReader : public PrimaryKeyIndexReader
{
public:
    MockPrimaryKeyIndexReader(const std::map<std::string, docid_t>& pkToDocIdMap)
        : PrimaryKeyIndexReader()
        , _pkToDocIdMap(pkToDocIdMap)
    {
        for (auto iter = _pkToDocIdMap.begin(); iter != _pkToDocIdMap.end(); ++iter) {
            _docIdToPkMap[iter->second] = iter->first;
        }
    }
    ~MockPrimaryKeyIndexReader() {}

public:
    MockPrimaryKeyIndexReader(const MockPrimaryKeyIndexReader&) = delete;
    MockPrimaryKeyIndexReader& operator=(const MockPrimaryKeyIndexReader&) = delete;
    MockPrimaryKeyIndexReader(MockPrimaryKeyIndexReader&&) = delete;
    MockPrimaryKeyIndexReader& operator=(MockPrimaryKeyIndexReader&&) = delete;

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override
    {
        assert(0);
        return Status::Unimplement();
    }

    docid_t Lookup(const std::string& pkStr) const override
    {
        if (_pkToDocIdMap.find(pkStr) != _pkToDocIdMap.end()) {
            return _pkToDocIdMap.at(pkStr);
        }
        return INVALID_DOCID;
    }
    docid_t Lookup(const std::string& pkStr, future_lite::Executor* executor) const override { return Lookup(pkStr); }
    docid_t Lookup(const autil::StringView& pkStr) const override { return INVALID_DOCID; }
    index::Result<indexlib::index::PostingIterator*> Lookup(const indexlib::index::Term& term,
                                                            uint32_t statePoolSize = 1000,
                                                            PostingType type = pt_default,
                                                            autil::mem_pool::Pool* sessionPool = NULL) override
    {
        assert(false);
        return nullptr;
    }
    void DeleteIfExist(const docid_t docId)
    {
        auto iter = _docIdToPkMap.find(docId);
        if (iter != _docIdToPkMap.end()) {
            _pkToDocIdMap.erase(iter->second);
            _docIdToPkMap.erase(iter);
        }
    }

    docid_t LookupWithPKHash(const autil::uint128_t& pkHash, future_lite::Executor* executor = nullptr) const override
    {
        assert(false);
        return INVALID_DOCID;
    }
    bool LookupWithPKHash(const autil::uint128_t& pkHash, segmentid_t specifySegment, docid_t* docid) const override
    {
        assert(false);
        return INVALID_DOCID;
    }

    std::shared_ptr<indexlibv2::index::AttributeReader> GetPKAttributeReader() const override
    {
        assert(false);
        return nullptr;
    }

    Status OpenWithoutPKAttribute(const std::shared_ptr<indexlibv2::config::IIndexConfig>&,
                                  const indexlibv2::framework::TabletData*) override
    {
        assert(false);
        return Status::Unimplement();
    }

private:
    std::map<std::string, docid_t> _pkToDocIdMap;
    std::map<docid_t, std::string> _docIdToPkMap;
};

}} // namespace indexlib::index
