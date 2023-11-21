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

#include <functional>
#include <string_view>

#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/util/KeyValueMap.h"

DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, MergePolicy);
DECLARE_REFERENCE_CLASS(table, TableMerger);
DECLARE_REFERENCE_CLASS(table, ExecutorProvider);

namespace indexlib { namespace table {

class TableFactory : public plugin::ModuleFactory
{
public:
    TableFactory();
    virtual ~TableFactory();

public:
    void destroy() override { delete this; }
    virtual TableWriter* CreateTableWriter(const util::KeyValueMap& parameters) = 0;
    virtual TableResource* CreateTableResource(const util::KeyValueMap& parameters) = 0;
    virtual TableMerger* CreateTableMerger(const util::KeyValueMap& parameters) = 0;
    virtual TableReader* CreateTableReader(const util::KeyValueMap& parameters) = 0;
    virtual std::function<void()> CreateCleanResourceTask(const util::KeyValueMap& parameters) { return {}; };
    // default return NULL
    virtual MergePolicy* CreateMergePolicy(const util::KeyValueMap& parameters);
    virtual ExecutorProvider* CreateExecutorProvider(const util::KeyValueMap& parameters);
    virtual void Close(const util::KeyValueMap& parameters);

    static constexpr std::string_view TABLE_FACTORY_NAME = "TableFactory";
    static constexpr std::string_view SCHEMA_NAME = "SchemaName";

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableFactory);
}} // namespace indexlib::table
