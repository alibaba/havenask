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
#include <string>

#include "indexlib/base/Constant.h"

namespace indexlibv2::index {

inline const std::string ANN_INDEX_TYPE_STR = "ann";
inline const std::string ANN_INDEX_PATH = "index";
inline const std::string OPTIMIZE_MERGE = "optmize_merge";

typedef float match_score_t;
struct ANNMatchItem {
    docid_t docid {INVALID_DOCID};
    match_score_t score {0.0f};
};

} // namespace indexlibv2::index