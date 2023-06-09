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
#include "suez_navi/resource/UdfToQueryManagerR.h"

namespace suez_navi {

const std::string UdfToQueryManagerR::RESOURCE_ID = "udf_to_query_manager_r";

UdfToQueryManagerR::UdfToQueryManagerR() {
}

UdfToQueryManagerR::~UdfToQueryManagerR() {
}

void UdfToQueryManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

bool UdfToQueryManagerR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode UdfToQueryManagerR::init(navi::ResourceInitContext &ctx) {
    isearch::sql::UdfToQueryManagerPtr manager(new isearch::sql::UdfToQueryManager());
    if (!manager->init()) {
        NAVI_KERNEL_LOG(ERROR, "init UdfToQueryManager failed");
        return navi::EC_ABORT;
    }
    _manager = manager;
    return navi::EC_NONE;
}

const isearch::sql::UdfToQueryManagerPtr &UdfToQueryManagerR::getManager() const {
    return _manager;
}

REGISTER_RESOURCE(UdfToQueryManagerR);

}

