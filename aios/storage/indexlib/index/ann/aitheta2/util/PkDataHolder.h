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

#include "autil/NoCopyable.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataWriter.h"

namespace indexlibv2::index::ann {

struct PkDataHeader {
    int64_t indexId {kDefaultIndexId};
    uint64_t count {0ul};
};

class PkDataIterator
{
public:
    PkDataIterator(const std::map<primary_key_t, docid_t>::const_iterator& iter,
                   const std::map<primary_key_t, docid_t>::const_iterator& end,
                   const std::vector<docid_t>* old2NewDocId = nullptr)
        : _iter(iter)
        , _end(end)
        , _old2NewDocId(old2NewDocId)
    {
    }
    ~PkDataIterator() = default;

public:
    const primary_key_t* pk() const { return &(_iter->first); }
    const docid_t* docId() const
    {
        return (nullptr != _old2NewDocId ? &(_old2NewDocId->at(_iter->second)) : &(_iter->second));
    }
    bool isValid() const { return _iter != _end; }
    void next() { ++_iter; }

private:
    std::map<primary_key_t, docid_t>::const_iterator _iter;
    std::map<primary_key_t, docid_t>::const_iterator _end;
    const std::vector<docid_t>* _old2NewDocId = nullptr;
};

class PkData
{
public:
    PkData() = default;
    ~PkData() = default;

public:
    void Add(primary_key_t pk, docid_t docId) { _pkData[pk] = docId; }
    bool Contains(primary_key_t pk) const { return _pkData.find(pk) != _pkData.end(); }
    bool Remove(primary_key_t pk, docid_t& docId);
    size_t Size() const { return _pkData.size(); }
    void SetDocIdMapper(const std::vector<docid_t>* old2NewDocId) { _old2NewDocId = old2NewDocId; }
    std::unique_ptr<PkDataIterator> CreateIterator() const
    {
        return std::make_unique<PkDataIterator>(_pkData.cbegin(), _pkData.cend(), _old2NewDocId);
    }
    docid_t Test_Lookup(primary_key_t pk) const;

private:
    std::map<primary_key_t, docid_t> _pkData;
    const std::vector<docid_t>* _old2NewDocId = nullptr;
};

class PkDataHolder : public autil::NoCopyable
{
public:
    PkDataHolder() = default;
    ~PkDataHolder() = default;

public:
    void Add(primary_key_t pk, docid_t docId, const std::vector<index_id_t>& indexIds);
    bool IsExist(primary_key_t pk) const;
    bool Remove(primary_key_t pk, docid_t& docId, std::vector<index_id_t>& indexIds);
    const std::map<index_id_t, PkData>& GetPkDataMap() const { return _pkDataMap; }
    size_t PkCount() const { return _pkCount; }
    size_t DeletedPkCount() const { return _deletedPkCount; }
    void Clear();
    void SetDocIdMapper(const std::vector<docid_t>* old2NewDocId);

private:
    std::map<index_id_t, PkData> _pkDataMap;
    size_t _pkCount {0};
    size_t _deletedPkCount {0};
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<PkDataHolder> PkDataHolderPtr;

} // namespace indexlibv2::index::ann
