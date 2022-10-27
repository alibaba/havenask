#ifndef __INDEXLIB_TABLE_FACTORY_H
#define __INDEXLIB_TABLE_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/plugin/ModuleFactory.h"

DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(table, TableReader);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, MergePolicy);
DECLARE_REFERENCE_CLASS(table, TableMerger);

IE_NAMESPACE_BEGIN(table);

class TableFactory : public plugin::ModuleFactory
{
public:
    TableFactory();
    virtual ~TableFactory();
public:
    void destroy() override
    {
        delete this;
    }
    virtual TableWriter* CreateTableWriter(const util::KeyValueMap& parameters) = 0;
    virtual TableResource* CreateTableResource(const util::KeyValueMap& parameters) = 0;
    virtual TableMerger* CreateTableMerger(const util::KeyValueMap& parameters) = 0;
    
    // default return NULL
    virtual MergePolicy* CreateMergePolicy(const util::KeyValueMap& parameters);
    virtual TableReader* CreateTableReader(const util::KeyValueMap& parameters) = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableFactory);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_FACTORY_H
