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

#include <map>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Term.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace search {

#define BITMAP_AUX_NAME "BITMAP"

enum QueryInsertType {
    QI_INVALID,
    QI_BEFORE,
    QI_AFTER,
    QI_OVERWRITE
};

enum SelectAuxChainType {
    SAC_INVALID,
    SAC_DF_BIGGER,
    SAC_DF_SMALLER,
    SAC_ALL
};

typedef std::map<common::Term, df_t> TermDFMap;

} // namespace search
} // namespace isearch
