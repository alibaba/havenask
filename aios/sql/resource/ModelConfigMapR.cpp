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
#include "sql/resource/ModelConfigMapR.h"

#include <engine/NaviConfigContext.h>
#include <map>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string ModelConfigMapR::RESOURCE_ID = "model_config_map_r";

ModelConfigMapR::ModelConfigMapR() {}

ModelConfigMapR::~ModelConfigMapR() {}

void ModelConfigMapR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool ModelConfigMapR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "model_config_map", _modelConfigMap, _modelConfigMap);
    return true;
}

navi::ErrorCode ModelConfigMapR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

const isearch::turing::ModelConfigMap &ModelConfigMapR::getModelConfigMap() const {
    return _modelConfigMap;
}

REGISTER_RESOURCE(ModelConfigMapR);

} // namespace sql
