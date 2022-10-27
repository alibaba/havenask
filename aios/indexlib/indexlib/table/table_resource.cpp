#include "indexlib/table/table_resource.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableResource);

TableResource::TableResource()
    : mMetricProvider(NULL)
{
}

TableResource::~TableResource() 
{
}

IE_NAMESPACE_END(table);

