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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/ThreadPool.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"

namespace indexlib::index {

class PrimaryKeyDuplicationChecker
{
public:
    PrimaryKeyDuplicationChecker(const PrimaryKeyIndexReader* pkReader);
    ~PrimaryKeyDuplicationChecker();

public:
    bool Start();

    template <typename Key>
    bool PushKey(Key key, docid_t docId);

    bool WaitFinish();

private:
    bool PushKeyImpl(const autil::uint128_t& pkHash, docid_t docId);

private:
    const PrimaryKeyIndexReader* _pkReader = nullptr;
    std::unique_ptr<autil::ThreadPool> _threadPool;
    std::vector<std::pair<autil::uint128_t, docid_t>> _pkHashs;

private:
    AUTIL_LOG_DECLARE();
};

template <typename Key>
inline bool PrimaryKeyDuplicationChecker::PushKey(Key key, docid_t docId)
{
    if constexpr (std::is_same_v<Key, autil::uint128_t>) {
        return PushKeyImpl(key, docId);
    } else {
        assert((std::is_same_v<Key, uint64_t>));
        autil::uint128_t pkHash(key);
        return PushKeyImpl(pkHash, docId);
    }
}

} // namespace indexlib::index
