#ifndef __INDEXLIB_OPERATION_DUMPER_H
#define __INDEXLIB_OPERATION_DUMPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_meta.h"
#include "indexlib/partition/operation_queue/operation_base.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(partition);

class OperationDumper
{
public:
    OperationDumper(const OperationMeta& opMeta)
        : mOperationMeta(opMeta)
        , mReleaseBlockAfterDump(true)
    {}
    
    ~OperationDumper() {}
    
public:
    void Init(const OperationBlockVec& opBlocks, bool releaseBlockAfterDump);
    void Dump(const file_system::DirectoryPtr& directory,
              autil::mem_pool::PoolBase* dumpPool = NULL);
    inline static size_t GetDumpSize(OperationBase* op)
    {
        return sizeof(uint8_t)         // 1 byte(operation type)
             + sizeof(int64_t)         // 8 byte timestamp 
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

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationDumper);

/////////////////////////////////////////////////////////////

inline size_t OperationDumper::DumpSingleOperation(
        OperationBase* op, char* buffer, size_t bufLen)
{
    char* begin = buffer;
    char *cur = begin;
    uint8_t opType = (uint8_t)op->GetSerializedType();
    *(uint8_t*)cur = opType;
    cur += sizeof(uint8_t);
    int64_t timestamp = op->GetTimestamp();
    *(int64_t*)cur = timestamp;
    cur += sizeof(timestamp);
    size_t dataLen =
        op->Serialize(cur, bufLen - (cur - begin));
    return (cur - begin) + dataLen;
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_DUMPER_H
