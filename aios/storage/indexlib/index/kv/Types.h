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

namespace indexlib::enum_namespace {
enum TableSearchCacheType {
    tsc_default,    // search cache when enable cache;
    tsc_no_cache,   // not search cache
    tsc_only_cache, // search result only in cache
};
} // namespace indexlib::enum_namespace

namespace indexlib::index::enum_namespace {
enum KVValueType {
    KVVT_TYPED,
    KVVT_PACKED_ONE_FIELD,
    KVVT_PACKED_MULTI_FIELD,
    KVVT_UNKNOWN,
};
enum class KVIndexType : int8_t { KIT_DENSE_HASH, KIT_CUCKOO_HASH, KIT_UNKOWN };
} // namespace indexlib::index::enum_namespace

namespace indexlib::index {
typedef int32_t regionid_t;
typedef uint32_t compact_keytype_t;
typedef uint64_t keytype_t;
} // namespace indexlib::index

namespace indexlib {
enum IndexPreferenceType {
    ipt_perf,
    ipt_store,
    ipt_unknown,
};

using namespace ::indexlib::enum_namespace;
} // namespace indexlib

//////////////////////////////////////////////////////////////////////
// TODO: rm
namespace indexlibv2::index {
using indexlib::index::compact_keytype_t;
using indexlib::index::keytype_t;
using indexlib::index::regionid_t;
using namespace ::indexlib::index::enum_namespace;
} // namespace indexlibv2::index

namespace indexlibv2 {
using namespace ::indexlib::enum_namespace;
} // namespace indexlibv2
