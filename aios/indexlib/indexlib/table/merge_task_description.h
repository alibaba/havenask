#ifndef __INDEXLIB_MERGE_TASK_DESCRIPTION_H
#define __INDEXLIB_MERGE_TASK_DESCRIPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(table);

class MergeTaskDescription : public autil::legacy::Jsonizable
{
public:
    MergeTaskDescription();
    ~MergeTaskDescription();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    std::string name;
    uint32_t cost;
    util::KeyValueMap parameters;
private:
    static std::string MERGE_TASK_NAME_CONFIG;
    static std::string MERGE_TASK_COST_CONFIG;
    static std::string MERGE_TASK_PARAM_CONFIG; 
private:
    IE_LOG_DECLARE();
};

typedef std::vector<MergeTaskDescription> MergeTaskDescriptions;

DEFINE_SHARED_PTR(MergeTaskDescription);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_MERGE_TASK_DESCRIPTION_H
