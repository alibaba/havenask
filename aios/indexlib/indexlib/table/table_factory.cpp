#include "indexlib/table/table_factory.h"
#include "indexlib/table/merge_policy.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableFactory);

TableFactory::TableFactory() 
{
}

TableFactory::~TableFactory() 
{
}

MergePolicy* TableFactory::CreateMergePolicy(const util::KeyValueMap& parameters)
{
    return NULL;
}

IE_NAMESPACE_END(table);

