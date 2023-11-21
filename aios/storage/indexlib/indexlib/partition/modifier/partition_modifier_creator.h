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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/modifier/partition_modifier.h"

namespace indexlib { namespace partition {

class PartitionModifierCreator
{
public:
    PartitionModifierCreator();
    ~PartitionModifierCreator();

public:
    static PartitionModifierPtr CreatePatchModifier(const config::IndexPartitionSchemaPtr& schema,
                                                    const index_base::PartitionDataPtr& partitionData, bool needPk,
                                                    bool enablePackFile);

    static PartitionModifierPtr CreateInplaceModifier(const config::IndexPartitionSchemaPtr& schema,
                                                      const IndexPartitionReaderPtr& reader);

    static PartitionModifierPtr CreateOfflineModifier(const config::IndexPartitionSchemaPtr& schema,
                                                      const index_base::PartitionDataPtr& partitionData, bool needPk,
                                                      bool enablePackFile);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionModifierCreator);
}} // namespace indexlib::partition
