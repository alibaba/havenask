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

#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/KKVDoc.h"

namespace indexlibv2::index {

struct SKeyCollectInfo {
    SKeyCollectInfo()
        : skey(0)
        , ts(0)
        , expireTime(indexlib::UNINITIALIZED_EXPIRE_TIME)
        , isDeletedPKey(false)
        , isDeletedSKey(false)
        , value(autil::StringView::empty_instance())
    {
    }

    uint64_t skey;
    uint32_t ts;
    uint32_t expireTime;
    bool isDeletedPKey;
    bool isDeletedSKey;
    autil::StringView value;

    // TODO: tmp function, delete SKeyCollectInfo, use KKVDoc directory
    void FromKKVDoc(bool isDeletedPKey_, const KKVDoc& doc)
    {
        skey = doc.skey;
        ts = doc.timestamp;
        expireTime = doc.expireTime;
        isDeletedSKey = doc.skeyDeleted;
        isDeletedPKey = isDeletedPKey_;
        value = doc.value;
    }
    // TODO: tmp function, delete SKeyCollectInfo, use KKVDoc directory
    std::pair<bool, KKVDoc> ToKKVDoc() const
    {
        KKVDoc doc;
        doc.skey = skey;
        doc.timestamp = ts;
        doc.expireTime = expireTime;
        doc.skeyDeleted = isDeletedSKey;
        doc.value = value;
        return {isDeletedPKey, doc};
    }
};

} // namespace indexlibv2::index
