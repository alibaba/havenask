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
#include "build_service/builder/SortDocumentConverter.h"

#include "autil/HashAlgorithm.h"
using namespace std;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, SortDocumentConverter);

SortDocumentConverter::SortDocumentConverter() : _pool(new mem_pool::Pool) {}

SortDocumentConverter::~SortDocumentConverter() {}

}} // namespace build_service::builder
