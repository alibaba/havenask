#include "indexlib/table/table_reader.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableReader);

TableReader::TableReader()
    : mInterfaceId(DEFAULT_INTERFACE_ID)
    , mForceSeekInfo(-1, -1)
{
}

TableReader::~TableReader() 
{
}

bool TableReader::Init(const IndexPartitionSchemaPtr& schema,
                       const IndexPartitionOptions& options)
{
    mSchema = schema;
    mOptions = options;
    InitInterfaceId();
    return DoInit();
}

bool TableReader::DoInit()
{
    return true;
}

void TableReader::SetSegmentDependency(const std::vector<BuildingSegmentReaderPtr>& inMemSegments)
{
    mDependentInMemSegments = inMemSegments;
}

IE_NAMESPACE_END(table);

