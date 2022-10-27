#ifndef __INDEXLIB_TABLE_IDENTIFIER_H
#define __INDEXLIB_TABLE_IDENTIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, TableIdentifierImpl);

IE_NAMESPACE_BEGIN(config);

class TableIdentifier
{
public:
    struct Partition {
        uint16_t from = 0;
        uint16_t to = 65535;
        Partition(uint16_t f, uint16_t t);
    };
    
public:
    TableIdentifier(const std::string& id, TableType type,
                    const std::string& name, TableIdentifier::Partition partition);
    ~TableIdentifier();

public:
    void SetId(const std::string& id);
    void SetTableName(const std::string& name);
    void SetTableType(TableType type);
    void SetPartition(const TableIdentifier::Partition& partition);

public:
    const std::string& GetId() const;
    const std::string& GetTableName() const;
    const TableType GetTableType() const;
    const TableIdentifier::Partition& GetPartition() const;

private:
    TableIdentifierImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableIdentifier);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TABLE_IDENTIFIER_H
