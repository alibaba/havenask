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
#include "indexlib/partition/operation_queue/operation_creator.h"

namespace indexlib { namespace partition {

class RemoveOperationCreator : public OperationCreator
{
public:
    RemoveOperationCreator(const config::IndexPartitionSchemaPtr& schema);
    ~RemoveOperationCreator();

public:
    bool Create(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                OperationBase** operation) override;

    autil::uint128_t GetPkHashFromOperation(OperationBase* operation, InvertedIndexType pkIndexType) const;

private:
    friend class RemoveOperationCreatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RemoveOperationCreator);
}} // namespace indexlib::partition
