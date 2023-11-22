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
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_meta.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace partition {

class OperationDumper
{
public:
    OperationDumper(const OperationMeta& opMeta)
        : mOperationMeta(opMeta)
        , mReleaseBlockAfterDump(true)
        , mDumpToDisk(false)
    {
    }

    ~OperationDumper() {}

public:
    void Init(const OperationBlockVec& opBlocks, bool releaseBlockAfterDump, bool dumpToDisk);
    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool = NULL);
    inline static size_t GetDumpSize(OperationBase* op)
    {
        return sizeof(uint8_t)   // 1 byte(operation type)
               + sizeof(int64_t) // 8 byte timestamp
               + op->GetSerializeSize();
    }

public:
    static size_t DumpSingleOperation(OperationBase* op, char* buffer, size_t bufLen);

private:
    void CheckOperationBlocks() const;

private:
    OperationMeta mOperationMeta;
    OperationBlockVec mOpBlockVec;
    bool mReleaseBlockAfterDump;
    bool mDumpToDisk;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationDumper);

/////////////////////////////////////////////////////////////

inline size_t OperationDumper::DumpSingleOperation(OperationBase* op, char* buffer, size_t bufLen)
{
    char* begin = buffer;
    char* cur = begin;
    uint8_t opType = (uint8_t)op->GetSerializedType();
    *(uint8_t*)cur = opType;
    cur += sizeof(uint8_t);
    int64_t timestamp = op->GetTimestamp();
    *(int64_t*)cur = timestamp;
    cur += sizeof(timestamp);
    size_t dataLen = op->Serialize(cur, bufLen - (cur - begin));
    return (cur - begin) + dataLen;
}
}} // namespace indexlib::partition
