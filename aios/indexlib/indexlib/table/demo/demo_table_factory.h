#ifndef __INDEXLIB_DEMO_TABLE_FACTORY_H
#define __INDEXLIB_DEMO_TABLE_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/table/table_factory.h"

DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, TableMerger);
DECLARE_REFERENCE_CLASS(table, MergePolicy);

IE_NAMESPACE_BEGIN(table);

class DemoTableFactory : public TableFactory
{
public:
    DemoTableFactory();
    ~DemoTableFactory();
public:
    TableWriter* CreateTableWriter(const util::KeyValueMap& parameters) override;
    TableResource* CreateTableResource(const util::KeyValueMap& parameters) override;
    TableMerger* CreateTableMerger(const util::KeyValueMap& parameters) override;
    MergePolicy* CreateMergePolicy(const util::KeyValueMap& parameters) override;
    TableReader* CreateTableReader(const util::KeyValueMap& parameters) override;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoTableFactory);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_DEMO_TABLE_FACTORY_H
