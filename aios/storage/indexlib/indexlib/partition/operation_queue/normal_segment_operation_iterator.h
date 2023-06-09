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
#ifndef __INDEXLIB_NORMAL_SEGMENT_OPERATION_ITERATOR_H
#define __INDEXLIB_NORMAL_SEGMENT_OPERATION_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/in_mem_segment_operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_factory.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

namespace indexlib { namespace partition {

class NormalSegmentOperationIterator : public InMemSegmentOperationIterator
{
public:
    NormalSegmentOperationIterator(const config::IndexPartitionSchemaPtr& schema, const OperationMeta& operationMeta,
                                   segmentid_t segmentId, size_t offset, int64_t timestamp)
        : InMemSegmentOperationIterator(schema, operationMeta, segmentId, offset, timestamp)
    {
    }

    ~NormalSegmentOperationIterator() {}

public:
    void Init(const file_system::DirectoryPtr& operationDirectory);

private:
    file_system::FileReaderPtr mFileReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalSegmentOperationIterator);
}}     // namespace indexlib::partition
#endif //__INDEXLIB_NORMAL_SEGMENT_OPERATION_ITERATOR_H
