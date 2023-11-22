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

#include <stdint.h>
#include <string>
#include <vector>

namespace indexlib::enum_namespace {
enum class IdMaskType {
    BUILD_PUBLIC,
    BUILD_PRIVATE,
};
} // namespace indexlib::enum_namespace

// global_enum_namespace will public to global namespace by indexlib.h, todo: rm it
namespace indexlib::global_enum_namespace {
enum DocOperateType {
    UNKNOWN_OP = 0,
    ADD_DOC = 1,
    DELETE_DOC = 2,
    UPDATE_FIELD = 4,
    SKIP_DOC = 8,
    DELETE_SUB_DOC = 16,
    CHECKPOINT_DOC = 32,
    ALTER_DOC = 64,
    BULKLOAD_DOC = 128
};
} // namespace indexlib::global_enum_namespace

namespace indexlib {
typedef int32_t docid_t;
typedef int32_t docid32_t;
typedef int64_t exdocid_t;
typedef int64_t globalid_t;
typedef int32_t fieldid_t;
typedef int32_t versionid_t;
typedef int32_t segmentid_t;
typedef uint32_t schemaid_t;
typedef uint16_t segmentindex_t;
typedef uint64_t dictkey_t;
typedef uint64_t offset_t;
typedef uint32_t short_offset_t;
typedef uint32_t schema_opid_t; // begin from 1
typedef std::pair<docid_t, docid_t> DocIdRange;
typedef std::vector<DocIdRange> DocIdRangeVector;
typedef int64_t docid64_t;

using namespace ::indexlib::enum_namespace;
using namespace ::indexlib::global_enum_namespace;
} // namespace indexlib
//////////////////////////////////////////////////////////////////////

// TODO: rm
namespace indexlibv2 {
using indexlib::dictkey_t;
using indexlib::docid32_t;
using indexlib::docid64_t;
using indexlib::docid_t;
using indexlib::DocIdRange;
using indexlib::DocIdRangeVector;
using indexlib::exdocid_t;
using indexlib::fieldid_t;
using indexlib::globalid_t;
using indexlib::offset_t;
using indexlib::schema_opid_t;
using indexlib::schemaid_t;
using indexlib::segmentid_t;
using indexlib::segmentindex_t;
using indexlib::short_offset_t;
using indexlib::versionid_t;

using namespace ::indexlib::enum_namespace;
using namespace ::indexlib::global_enum_namespace;
} // namespace indexlibv2
