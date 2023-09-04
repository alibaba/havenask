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

#include <string>

#include "ha3/turing/common/ModelConfig.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class ModelConfigMapR : public navi::Resource {
public:
    ModelConfigMapR();
    ~ModelConfigMapR();
    ModelConfigMapR(const ModelConfigMapR &) = delete;
    ModelConfigMapR &operator=(const ModelConfigMapR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const isearch::turing::ModelConfigMap &getModelConfigMap() const;

public:
    static const std::string RESOURCE_ID;

private:
    isearch::turing::ModelConfigMap _modelConfigMap;
};

NAVI_TYPEDEF_PTR(ModelConfigMapR);

} // namespace sql
