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

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document { namespace legacy {

class KVIndexDocument
{
public:
    KVIndexDocument(autil::mem_pool::Pool* pool = NULL);
    ~KVIndexDocument();

public:
    KVIndexDocument& operator=(const KVIndexDocument& other);
    uint64_t GetPkeyHash() const { return mPkeyHash; }
    uint64_t GetSkeyHash() const { return mSkeyHash; }
    void SetPkeyHash(uint64_t key) { mPkeyHash = key; }
    void SetSkeyHash(uint64_t key)
    {
        mHasSkey = true;
        mSkeyHash = key;
    }
    bool HasSkey() const { return mHasSkey; }
    void serialize(autil::DataBuffer& dataBuffer) const;

    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool,
                     uint32_t docVersion = LEGACY_DOCUMENT_BINARY_VERSION);

    bool operator==(const KVIndexDocument& right) const
    {
        return mPkeyHash == right.mPkeyHash && mSkeyHash == right.mSkeyHash && mHasSkey == right.mHasSkey;
    }

    bool operator!=(const KVIndexDocument& right) const { return !(operator==(right)); }

    void Reset()
    {
        mPkeyHash = 0;
        mSkeyHash = 0;
        mHasSkey = false;
    }

private:
    uint64_t mPkeyHash;
    uint64_t mSkeyHash;
    bool mHasSkey;

private:
};

DEFINE_SHARED_PTR(KVIndexDocument);

}}} // namespace indexlib::document::legacy
