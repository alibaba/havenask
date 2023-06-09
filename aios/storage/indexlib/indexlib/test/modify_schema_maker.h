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
#ifndef __INDEXLIB_MODIFY_SCHEMA_MAKER_H
#define __INDEXLIB_MODIFY_SCHEMA_MAKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace test {

class ModifySchemaMaker
{
public:
    ModifySchemaMaker();
    ~ModifySchemaMaker();

public:
    static void AddModifyOperations(const config::IndexPartitionSchemaPtr& schema,
                                    const std::string& deleteIndexInfo, /* fields=a,b,c;indexs=i1,i2;attributes=a1 */
                                    const std::string& addFieldNames, const std::string& addIndexNames,
                                    const std::string& addAttributeNames);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ModifySchemaMaker);
}} // namespace indexlib::test

#endif //__INDEXLIB_MODIFY_SCHEMA_MAKER_H
