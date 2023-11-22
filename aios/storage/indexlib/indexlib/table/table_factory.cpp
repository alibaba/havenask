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
#include "indexlib/table/table_factory.h"

#include <iosfwd>

#include "indexlib/table/executor_provider.h"
#include "indexlib/table/merge_policy.h"

using namespace std;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, TableFactory);

TableFactory::TableFactory() {}

TableFactory::~TableFactory() {}

MergePolicy* TableFactory::CreateMergePolicy(const util::KeyValueMap& parameters) { return nullptr; }

ExecutorProvider* TableFactory::CreateExecutorProvider(const util::KeyValueMap& parameters) { return nullptr; }
void TableFactory::Close(const util::KeyValueMap& parameters) {}

}} // namespace indexlib::table
