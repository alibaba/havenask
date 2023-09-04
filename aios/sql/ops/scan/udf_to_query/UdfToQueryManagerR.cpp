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
#include "sql/ops/scan/udf_to_query/UdfToQueryManagerR.h"

#include <memory>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string UdfToQueryManagerR::RESOURCE_ID = "udf_to_query_manager_r";

UdfToQueryManagerR::UdfToQueryManagerR() {}

UdfToQueryManagerR::~UdfToQueryManagerR() {}

void UdfToQueryManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool UdfToQueryManagerR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode UdfToQueryManagerR::init(navi::ResourceInitContext &ctx) {
    UdfToQueryManagerPtr manager(new UdfToQueryManager());
    if (!manager->init()) {
        SQL_LOG(ERROR, "init UdfToQueryManager failed");
        return navi::EC_ABORT;
    }
    _manager = manager;
    return navi::EC_NONE;
}

const UdfToQueryManagerPtr &UdfToQueryManagerR::getManager() const {
    return _manager;
}

REGISTER_RESOURCE(UdfToQueryManagerR);

} // namespace sql
