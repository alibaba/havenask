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
#include "indexlib/index/kkv/dump/TruncateKKVDocSorter.h"

#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/dump/SKeyHashComparator.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.table, TruncateKKVDocSorter);

bool TruncateKKVDocSorter::Init()
{
    assert(_kkvIndexConfig->NeedSuffixKeyTruncate());
    _skeyCountLimits = _kkvIndexConfig->GetSuffixKeyTruncateLimits();
    _docComparator.Init(_kkvIndexConfig);
    _skeyFieldType = _kkvIndexConfig->GetSKeyDictKeyType();
    return true;
}

std::vector<std::pair<bool, KKVDoc>> TruncateKKVDocSorter::Sort()
{
    // assert(_validSkeyCount <= (uint32_t)_collectInfos.size());
    uint32_t flushCount = _collectInfos.size();
    if (_validSkeyCount > _skeyCountLimits) {
        uint32_t deleteInfoCount = _collectInfos.size() - _validSkeyCount;
        flushCount = deleteInfoCount + _skeyCountLimits;
        auto beginIter = _collectInfos.begin();
        auto endIter = _collectInfos.end();

        bool skipFirstNode = false;
        // if first skey node is delete pkey node, skip it
        if ((*beginIter)->isDeletedPKey) {
            skipFirstNode = true;
            beginIter++;
            flushCount--;
        }

        // get flushCount elements
        if (flushCount > 0 && beginIter + flushCount < endIter) {
            std::nth_element(beginIter, beginIter + flushCount - 1, endIter, _docComparator);
        }
        beginIter = skipFirstNode ? _collectInfos.begin() + 1 : _collectInfos.begin();
        switch (_skeyFieldType) {
#define CASE_MACRO(fieldType)                                                                                          \
    case fieldType: {                                                                                                  \
        SKeyHashComparator<typename indexlib::index::FieldTypeTraits<fieldType>::AttrItemType> comp;                   \
        sort(beginIter, beginIter + flushCount, comp);                                                                 \
        break;                                                                                                         \
    }
            KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(_skeyFieldType));
            assert(false);
        }
        }

        if (skipFirstNode) {
            ++flushCount;
        }
    }
    _collectInfos.resize(flushCount);
    return ToKKVDocs(_collectInfos);
}
} // namespace indexlibv2::index
