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
#include "ha3/sql/resource/SqlRequestInfoR.h"

namespace isearch {
namespace sql {


const std::string SqlRequestInfoR::RESOURCE_ID = "SQL_REQUEST_INFO_R";

SqlRequestInfoR::SqlRequestInfoR() {
}

SqlRequestInfoR::~SqlRequestInfoR() {
}

void SqlRequestInfoR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

bool SqlRequestInfoR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode SqlRequestInfoR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

REGISTER_RESOURCE(SqlRequestInfoR);

}
}

