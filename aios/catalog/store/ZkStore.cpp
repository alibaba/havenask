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
#include "catalog/store/ZkStore.h"

#include "autil/Log.h"
#include "catalog/store/StoreFactory.h"
#include "worker_framework/CompressedWorkerState.h"
#include "worker_framework/PathUtil.h"
#include "worker_framework/WorkerState.h"
#include "worker_framework/ZkState.h"

using namespace worker_framework;

namespace catalog {

AUTIL_DECLARE_AND_SETUP_LOGGER(catalog, ZkStore);

ZkStore::ZkStore() = default;

ZkStore::~ZkStore() = default;

REGISTER_STORE(ZkStore, "zfs", []() { return std::make_unique<ZkStore>(); });

} // namespace catalog
