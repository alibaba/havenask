#ifndef __INDEXLIB_TASK_EXECUTE_META_H
#define __INDEXLIB_TASK_EXECUTE_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(table);

class TaskExecuteMeta : public autil::legacy::Jsonizable
{
public:
    TaskExecuteMeta(uint32_t _planIdx, uint32_t _taskIdx, uint32_t _cost)
        : planIdx(_planIdx)
        , taskIdx(_taskIdx)
        , cost(_cost) {}
    TaskExecuteMeta()
        : planIdx(0)
        , taskIdx(0)
        , cost(0) {}
    ~TaskExecuteMeta() {}

public:    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator == (const TaskExecuteMeta& other) const;

    std::string GetIdentifier() const;
    static bool GetIdxFromIdentifier(
        const std::string& taskName, uint32_t& planIdx, uint32_t& taskIdx);

private:
    static std::string TASK_IDENTIFY_PREFIX;
    
public: 
    uint32_t planIdx;
    uint32_t taskIdx;
    uint32_t cost;    
};
typedef std::vector<TaskExecuteMeta> TaskExecuteMetas;

DEFINE_SHARED_PTR(TaskExecuteMeta);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TASK_EXECUTE_META_H
