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
#include "autil/Diagnostic.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wsuggest-override")
#include <aitheta2/index_framework.h>
DIAGNOSTIC_POP

#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"

namespace indexlibv2::index::ann {

typedef aitheta2::IndexBuilder::Pointer AiThetaBuilderPtr;
typedef aitheta2::IndexSearcher::Pointer AiThetaSearcherPtr;
typedef aitheta2::IndexStreamer::Pointer AiThetaStreamerPtr;
typedef aitheta2::IndexFactory AiThetaFactory;
typedef aitheta2::IndexStorage::Pointer AiThetaStoragePtr;
typedef aitheta2::IndexContext AiThetaContext;
typedef aitheta2::IndexMeasure::Pointer AiThetaMeasurePtr;
typedef aitheta2::IndexParams AiThetaParams;
typedef aitheta2::IndexMeta AiThetaMeta;
typedef aitheta2::IndexQueryMeta IndexQueryMeta;
typedef aitheta2::IndexMeta::FeatureTypes FeatureType;
typedef aitheta2::IndexMeta::MajorOrders MajorOrder;
typedef aitheta2::IndexError IndexError;

typedef std::shared_ptr<float> embedding_t;
typedef int64_t index_id_t;
typedef int64_t key_t;
typedef int64_t primary_key_t;
typedef std::function<bool(uint64_t)> IndexContextFilter;
typedef std::map<index_id_t, size_t> DocCountMap;
typedef std::set<index_id_t> MergeTask;

#define ANN_CHECK(status, format, args...)                                                                             \
    do {                                                                                                               \
        if (!(status)) {                                                                                               \
            ALOG_LOG(_logger, alog::LOG_LEVEL_ERROR, format, ##args);                                                  \
            return false;                                                                                              \
        }                                                                                                              \
    } while (0)

#define ANN_CHECK_OK(error, format, args...)                                                                           \
    do {                                                                                                               \
        int ec = (error);                                                                                              \
        if (ec != 0) {                                                                                                 \
            ALOG_LOG(_logger, alog::LOG_LEVEL_ERROR, "aitheta2 error[%s], " format, aitheta2::IndexError::What(ec),    \
                     ##args);                                                                                          \
            return false;                                                                                              \
        }                                                                                                              \
    } while (0)

} // namespace indexlibv2::index::ann
