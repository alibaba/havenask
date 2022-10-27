#include "indexlib/table/task_execute_meta.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(table);

std::string TaskExecuteMeta::TASK_IDENTIFY_PREFIX = "__merge_task_";

void TaskExecuteMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("planIdx", planIdx);
    json.Jsonize("taskIdx", taskIdx);
    json.Jsonize("cost", cost); 
}

bool TaskExecuteMeta::operator == (const TaskExecuteMeta& other) const
{
    return planIdx == other.planIdx
        && taskIdx == other.taskIdx
        && cost == other.cost;
}

string TaskExecuteMeta::GetIdentifier() const
{
    return "__merge_task_" + autil::StringUtil::toString(planIdx)
        + "#" + autil::StringUtil::toString(taskIdx);
}

bool TaskExecuteMeta::GetIdxFromIdentifier(
        const std::string& taskName, uint32_t& planIdx, uint32_t& taskIdx)
{
    size_t posBegin = taskName.find(TASK_IDENTIFY_PREFIX);
    if (posBegin == string::npos)
    {
        return false;
    }
    StringTokenizer st(taskName.substr(posBegin + TASK_IDENTIFY_PREFIX.size()),
                       "#", StringTokenizer::TOKEN_TRIM | 
                       StringTokenizer::TOKEN_IGNORE_EMPTY);

    if (st.getNumTokens() != 2)
    {
        return false;
    }
    if (!StringUtil::fromString(st[0], planIdx))
    {
        return false;
    } 
    if (!StringUtil::fromString(st[1], taskIdx)) 
    {
        return false; 
    } 
    return true;
}

IE_NAMESPACE_END(table);

