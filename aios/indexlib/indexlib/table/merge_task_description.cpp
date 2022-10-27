#include "indexlib/table/merge_task_description.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, MergeTaskDescription);

std::string MergeTaskDescription::MERGE_TASK_NAME_CONFIG = "name";
std::string MergeTaskDescription::MERGE_TASK_COST_CONFIG = "cost";
std::string MergeTaskDescription::MERGE_TASK_PARAM_CONFIG = "parameters";

MergeTaskDescription::MergeTaskDescription()
    : name("")
    , cost(0)
{
}

MergeTaskDescription::~MergeTaskDescription() 
{
}

void MergeTaskDescription::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(MERGE_TASK_NAME_CONFIG, name);
    json.Jsonize(MERGE_TASK_COST_CONFIG, cost);
    json.Jsonize(MERGE_TASK_PARAM_CONFIG, parameters);
}

IE_NAMESPACE_END(table);

