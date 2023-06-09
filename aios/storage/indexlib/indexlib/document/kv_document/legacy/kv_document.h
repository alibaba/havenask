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
#ifndef __INDEXLIB_LEGACY_KV_DOCUMENT_H
#define __INDEXLIB_LEGACY_KV_DOCUMENT_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document { namespace legacy {

class KVDocument
{
public:
    KVDocument(autil::mem_pool::Pool* pool) : mPool(pool) {}
    ~KVDocument() {}

public:
    void SetKVIndexDocument(const KVIndexDocument& kvIndexDoc) { mKVIndexDocument = kvIndexDoc; }
    void SetValue(const autil::StringView& value) { mValue = autil::MakeCString(value.data(), value.size(), mPool); }

    void SetOperateType(DocOperateType op) { mOpType = op; }
    DocOperateType GetOperateType() const { return mOpType; }

    bool DeleteAllPkey() { return mOpType == DELETE_DOC && !mKVIndexDocument.HasSkey(); }
    void SetTimestamp(int64_t timestamp) { mTimestamp = timestamp; }
    void SetUserTimestamp(int64_t timestamp) { mUserTimestamp = timestamp; }
    int64_t GetTimestamp() const { return mTimestamp; }
    int64_t GetUserTimestamp() const { return mUserTimestamp; }
    const KVIndexDocument& GetKVIndexDocument() const { return mKVIndexDocument; }
    const autil::StringView& GetValue() const { return mValue; }
    size_t GetMemoryUse() { return sizeof(KVDocument) + mValue.size(); }

private:
    KVIndexDocument mKVIndexDocument;
    autil::StringView mValue;
    int64_t mTimestamp;
    int64_t mUserTimestamp;
    DocOperateType mOpType;
    autil::mem_pool::Pool* mPool;
};

DEFINE_SHARED_PTR(KVDocument);

}}} // namespace indexlib::document::legacy

#endif //__INDEXLIB_KV_DOCUMENT_H
