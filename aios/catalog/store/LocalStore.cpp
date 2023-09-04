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
#include "catalog/store/LocalStore.h"

#include "catalog/store/StoreFactory.h"
#include "fslib/fslib.h"
#include "fslib/util/FileUtil.h"

using namespace fslib::util;

namespace catalog {

AUTIL_DECLARE_AND_SETUP_LOGGER(catalog, LocalStore);

LocalStore::LocalStore() = default;

LocalStore::~LocalStore() = default;

REGISTER_STORE(LocalStore, "LOCAL", []() { return std::make_unique<LocalStore>(); });

} // namespace catalog
