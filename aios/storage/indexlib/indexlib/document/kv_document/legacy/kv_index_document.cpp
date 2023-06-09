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
#include "indexlib/document/kv_document/legacy/kv_index_document.h"

using namespace std;

namespace indexlib { namespace document { namespace legacy {

KVIndexDocument::KVIndexDocument(autil::mem_pool::Pool* pool) : mPkeyHash(0), mSkeyHash(0), mHasSkey(false) {}

KVIndexDocument::~KVIndexDocument() {}

KVIndexDocument& KVIndexDocument::operator=(const KVIndexDocument& other)
{
    mPkeyHash = other.mPkeyHash;
    mSkeyHash = other.mSkeyHash;
    mHasSkey = other.mHasSkey;
    return *this;
}

void KVIndexDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(mPkeyHash);
    dataBuffer.write(mSkeyHash);
    dataBuffer.write(mHasSkey);
}

void KVIndexDocument::deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool, uint32_t docVersion)
{
    dataBuffer.read(mPkeyHash);
    dataBuffer.read(mSkeyHash);
    dataBuffer.read(mHasSkey);
}

}}} // namespace indexlib::document::legacy
