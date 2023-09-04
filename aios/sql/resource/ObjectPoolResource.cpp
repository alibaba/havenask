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
#include "sql/resource/ObjectPoolResource.h"

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string ObjectPoolResource::RESOURCE_ID = "ObjectPoolResource";

ObjectPoolResource::ObjectPoolResource() {
    _objectPoolPtr.reset(new ObjectPool());
    _objectPoolReadOnlyPtr.reset(new ObjectPoolReadOnly());
}

ObjectPoolResource::~ObjectPoolResource() {}

void ObjectPoolResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool ObjectPoolResource::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode ObjectPoolResource::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

// DO NOT REGISTER THIS RESOURCE
// this resource is created by outter biz
} // namespace sql
