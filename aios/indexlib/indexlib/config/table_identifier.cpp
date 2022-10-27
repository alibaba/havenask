#include "indexlib/config/table_identifier.h"
#include "indexlib/config/impl/table_identifier_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);

TableIdentifier::TableIdentifier(const string& id, TableType type,
                                 const string& name, TableIdentifier::Partition partition)
    : mImpl(new TableIdentifierImpl(id, type, name, partition))
{}

void TableIdentifier::SetId(const std::string& id)
{
    mImpl->mId = id;
}

void TableIdentifier::SetTableName(const std::string& name)
{
    mImpl->mName = name;
}

void TableIdentifier::SetTableType(TableType type)
{
    mImpl->mType = type;
}

void TableIdentifier::SetPartition(const TableIdentifier::Partition& partition)
{
    mImpl->mPartition = partition;
}

const std::string& TableIdentifier::GetId() const
{
    return mImpl->mId;
}

const std::string& TableIdentifier::GetTableName() const
{
    return mImpl->mName;
}

const TableType TableIdentifier::GetTableType() const
{
    return mImpl->mType;
}

const TableIdentifier::Partition& TableIdentifier::GetPartition() const
{
    return mImpl->mPartition;
}

IE_NAMESPACE_END(config);
