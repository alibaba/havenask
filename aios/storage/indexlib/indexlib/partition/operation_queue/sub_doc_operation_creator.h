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

class SubDocOperationCreator : public OperationCreator
{
public:
    SubDocOperationCreator(const config::IndexPartitionSchemaPtr& schema, OperationCreatorPtr mainCreator,
                           OperationCreatorPtr subCreator);
    ~SubDocOperationCreator();

public:
    bool Create(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                OperationBase** operation) override;

private:
    bool CreateMainOperation(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                             OperationBase** operation);
    OperationBase** CreateSubOperation(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                                       size_t& subOperationCount);

private:
    OperationCreatorPtr mMainCreator;
    OperationCreatorPtr mSubCreator;
    InvertedIndexType mMainPkType;
    InvertedIndexType mSubPkType;

private:
    friend class SubDocOperationCreatorTest;
    friend class OperationFactoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocOperationCreator);
}} // namespace indexlib::partition
