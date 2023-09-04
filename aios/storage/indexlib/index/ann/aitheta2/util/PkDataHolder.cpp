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
#include "indexlib/index/ann/aitheta2/util/PkDataHolder.h"

namespace indexlibv2::index::ann {
AUTIL_LOG_SETUP(indexlib.index, PkDataHolder);

inline bool PkData::Remove(primary_key_t pk, docid_t& docId)
{
    auto iter = _pkData.find(pk);
    if (iter != _pkData.end()) {
        docId = iter->second;
        _pkData.erase(iter);
        return true;
    } else {
        return false;
    }
}

docid_t PkData::Test_Lookup(primary_key_t pk) const
{
    auto iter = _pkData.find(pk);
    if (iter == _pkData.end()) {
        return INVALID_DOCID;
    }
    auto docId = iter->second;
    if (nullptr != _old2NewDocId) {
        return _old2NewDocId->at(docId);
    } else {
        return docId;
    }
}

void PkDataHolder::Add(primary_key_t pk, docid_t docId, const std::vector<index_id_t>& indexIds)
{
    for (index_id_t indexId : indexIds) {
        auto iter = _pkDataMap.find(indexId);
        if (iter == _pkDataMap.end()) {
            _pkDataMap[indexId] = PkData();
        }
        _pkDataMap[indexId].Add(pk, docId);
    }
    ++_pkCount;
}

bool PkDataHolder::IsExist(primary_key_t pk) const
{
    for (auto& [_, pkData] : _pkDataMap) {
        if (pkData.Contains(pk)) {
            return true;
        }
    }
    return false;
}

bool PkDataHolder::Remove(primary_key_t pk, docid_t& docId, std::vector<index_id_t>& indexIds)
{
    docId = INVALID_DOCID;
    for (auto& [indexId, pkData] : _pkDataMap) {
        if (pkData.Remove(pk, docId)) {
            indexIds.push_back(indexId);
        }
    }

    if (docId != INVALID_DOCID) {
        --_pkCount;
        ++_deletedPkCount;
        return true;
    }

    indexIds.clear();
    return false;
}

void PkDataHolder::Clear()
{
    _pkDataMap.clear();
    _pkCount = 0;
    _deletedPkCount = 0;
}

void PkDataHolder::SetDocIdMapper(const std::vector<docid_t>* old2NewDocId)
{
    if (nullptr == old2NewDocId) {
        AUTIL_LOG(WARN, "old2NewDocId is nullptr");
        return;
    }
    for (auto& [_, pkData] : _pkDataMap) {
        pkData.SetDocIdMapper(old2NewDocId);
    }
}

} // namespace indexlibv2::index::ann