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

#include "indexlib/index/kkv/common/KKVDoc.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/dump/KKVDocFieldComparator.h"
#include "indexlib/index/kkv/dump/SKeyCollectInfo.h"
#include "indexlib/index/kkv/dump/SKeyCollectInfoPool.h"

namespace indexlibv2::index {

class KKVDocSorterBase
{
protected:
    using SKeyCollectInfos = std::vector<SKeyCollectInfo*>;

public:
    KKVDocSorterBase(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig)
        : _validSkeyCount(0)
        , _kkvIndexConfig(kkvIndexConfig)
    {
    }
    virtual ~KKVDocSorterBase() = default;

public:
    virtual bool Init() = 0;
    virtual std::vector<std::pair<bool, KKVDoc>> Sort() = 0;
    virtual bool KeepSkeySorted() const { return false; }

public:
    void AddDoc(bool isPkeyDeleted, const KKVDoc& doc)
    {
        if (!isPkeyDeleted && !doc.skeyDeleted) {
            ++_validSkeyCount;
        }

        // TODO: use KKVDoc & KKDocs
        auto info = _collectInfoPool.CreateCollectInfo();
        info->FromKKVDoc(isPkeyDeleted, doc);
        _collectInfos.push_back(info);
    }

    size_t Size() const { return _collectInfos.size(); }

    void Reset()
    {
        _validSkeyCount = 0;
        _collectInfos.clear();
        _collectInfoPool.Reset();
    }

    SKeyCollectInfos TEST_GetCollectInfos() const { return _collectInfos; }

protected:
    std::vector<std::pair<bool, KKVDoc>> ToKKVDocs(const SKeyCollectInfos& collectInfos) const
    {
        std::vector<std::pair<bool, KKVDoc>> ret;
        for (const auto info : collectInfos) {
            ret.push_back(info->ToKKVDoc());
        }
        return ret;
    }

protected:
    uint32_t _validSkeyCount;
    SKeyCollectInfos _collectInfos;
    SKeyCollectInfoPool _collectInfoPool;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _kkvIndexConfig;
};
} // namespace indexlibv2::index
