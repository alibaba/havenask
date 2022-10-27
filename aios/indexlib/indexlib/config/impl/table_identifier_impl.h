#ifndef __INDEXLIB_TABLE_IDENTIFIER_IMPL_H
#define __INDEXLIB_TABLE_IDENTIFIER_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/table_identifier.h"

IE_NAMESPACE_BEGIN(config);

class TableIdentifierImpl
{
private:
    friend class TableIdentifier;

public:
    TableIdentifierImpl(const std::string& id, TableType type,
                        const std::string& name, TableIdentifier::Partition partition)
        : mType(type)
        , mId(id)
        , mName(name)
        , mPartition(partition)
    {}
    ~TableIdentifierImpl() {}
    
private:
    TableType mType;
    std::string mId;
    std::string mName;
    TableIdentifier::Partition mPartition;
};

DEFINE_SHARED_PTR(TableIdentifierImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TABLE_IDENTIFIER_IMPL_H
