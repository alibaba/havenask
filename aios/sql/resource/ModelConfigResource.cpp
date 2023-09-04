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
#include "sql/resource/ModelConfigResource.h"

#include <iosfwd>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;

namespace sql {
AUTIL_LOG_SETUP(sql, ModelConfigResource);

const std::string ModelConfigResource::RESOURCE_ID = "ModelConfigResource";

ModelConfigResource::ModelConfigResource() {}

ModelConfigResource::ModelConfigResource(isearch::turing::ModelConfigMap *modelConfigMap)
    : _modelConfigMap(modelConfigMap) {}
void ModelConfigResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool ModelConfigResource::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode ModelConfigResource::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

REGISTER_RESOURCE(ModelConfigResource);

} // namespace sql
