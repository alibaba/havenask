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

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class IndexPartitionSchemaImpl;
typedef std::shared_ptr<IndexPartitionSchemaImpl> IndexPartitionSchemaImplPtr;
} // namespace indexlib::config
namespace indexlib::config {
class DictionarySchema;
typedef std::shared_ptr<DictionarySchema> DictionarySchemaPtr;
} // namespace indexlib::config
namespace indexlib::config {
class DictionaryConfig;
typedef std::shared_ptr<DictionaryConfig> DictionaryConfigPtr;
} // namespace indexlib::config
namespace indexlib::config {
class FieldConfig;
typedef std::shared_ptr<FieldConfig> FieldConfigPtr;
} // namespace indexlib::config
namespace indexlib::config {
class RegionSchema;
typedef std::shared_ptr<RegionSchema> RegionSchemaPtr;
} // namespace indexlib::config
namespace indexlib::config {
class AdaptiveDictionarySchema;
typedef std::shared_ptr<AdaptiveDictionarySchema> AdaptiveDictionarySchemaPtr;
} // namespace indexlib::config
namespace indexlib::test {
class ModifySchemaMaker;
typedef std::shared_ptr<ModifySchemaMaker> ModifySchemaMakerPtr;
} // namespace indexlib::test

namespace indexlib { namespace config {

class SchemaConfigurator
{
public:
    void Configurate(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema, bool isSubSchema);

private:
    AdaptiveDictionarySchemaPtr LoadAdaptiveDictSchema(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);
    DictionarySchemaPtr LoadDictSchema(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);
    std::shared_ptr<DictionaryConfig> LoadDictionaryConfig(const autil::legacy::Any& any,
                                                           IndexPartitionSchemaImpl& schema);

    void LoadRegions(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);

    RegionSchemaPtr LoadRegionSchema(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema,
                                     bool multiRegionFormat);

    void LoadModifyOperations(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);

    static void LoadOneModifyOperation(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);

    void LoadCustomizedConfig(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);
    static void AddOfflineJoinVirtualAttribute(const std::string& attrName, IndexPartitionSchemaImpl& destSchema);

private:
    friend class test::ModifySchemaMaker;
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::config
