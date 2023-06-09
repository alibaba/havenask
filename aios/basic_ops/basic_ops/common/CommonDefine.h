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
#ifndef ISEARCH_BASIC_OPS_COMMON_COMMONDEFINE_H
#define ISEARCH_BASIC_OPS_COMMON_COMMONDEFINE_H

#include <stddef.h>
#include <stdint.h>
#include <string>
#include "matchdoc/VectorDocStorage.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/platform/byte_order.h"
#include "basic_ops/common/CommonConstDefine.h"

typedef int32_t BE_MATCH_TYPE;
static const std::string MOUNT_TABLE_GROUP = "__mount_table__";
static const std::string kTuringAliasPrefix = "__turing_alias__";
static const std::string kJson2 = "json2";

static const std::string kSessionPoolCacheName = "sessionPoolCache";
static const std::string kQueryPoolCacheName = "queryPoolCache";
static const std::string kSessionTableMountInfoName = "sessionTableMountInfo";
static const std::string kQueryTableMountInfoName = "queryTableMountInfo";
static const std::string USER_CACHE_NAME = "user_cache";

enum IndexQueryParserType { T_KEYWORDS, T_LIST, T_UNKNOWN };

inline static IndexQueryParserType IndexQueryParserTypeFromString(const std::string &str) {
    if ("keywords" == str) {
        return T_KEYWORDS;
    } else if ("list" == str) {
        return T_LIST;
    } else {
        return T_UNKNOWN;
    }
}

inline std::string IndexQueryParserTypeToString(IndexQueryParserType type) {
    switch (type) {
        case T_KEYWORDS:
            return "keywords";
        case T_LIST:
            return "list";
        default:
            return "unknown";
    }
}

#define GET_KVPAIR_VARIANT(idx, in)                                           \
    auto kvPairTensor##idx = ctx->input(idx).scalar<tensorflow::Variant>()(); \
    auto in = kvPairTensor##idx.get<KvPairVariant>();                         \
    OP_REQUIRES(ctx, nullptr != in, errors::Unavailable("param[", idx, "] is nullptr"));

#define GET_INPUT_MATCHDOCS(idx, in)                                             \
    auto matchDocsTensor##idx = ctx->input(idx).scalar<tensorflow::Variant>()(); \
    auto in = matchDocsTensor##idx.get<MatchDocsVariant>();                      \
    OP_REQUIRES(ctx, nullptr != in, errors::Unavailable("input[", idx, "] is nullptr"));

#define SET_OUTPUT_MATCHDOCS(idx, out)                                                                     \
    {                                                                                                      \
        tensorflow::Tensor *outputTensor = nullptr;                                                        \
        tensorflow::Status status = ctx->allocate_output(idx, tensorflow::TensorShape({}), &outputTensor); \
        OP_REQUIRES_OK(ctx, status);                                                                       \
        outputTensor->scalar<tensorflow::Variant>()() = std::move(out);                                    \
    }

#endif  // ISEARCH_BASIC_OPS_COMMON_COMMONDEFINE_H
