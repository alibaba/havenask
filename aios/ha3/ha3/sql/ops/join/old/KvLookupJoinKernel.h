#ifndef ISEARCH_KV_LOOKUP_JOIN_KERNEL_H
#define ISEARCH_KV_LOOKUP_JOIN_KERNEL_H

#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/join/LookupJoinKernel.h>
#include <ha3/sql/ops/join/ValueCollector.h>
#include <matchdoc/MatchDocAllocator.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>

BEGIN_HA3_NAMESPACE(sql);

class KvLookupJoinKernel : public LookupJoinKernel {
public:
    KvLookupJoinKernel();
    ~KvLookupJoinKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
private:
    navi::ErrorCode doInit(SqlBizResource *bizResource, SqlQueryResource *queryResource) override;
    static matchdoc::MatchDocAllocatorPtr createMatchDocAllocator(
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            const std::shared_ptr<autil::mem_pool::Pool> &poolPtr);
    static void InsertPackAttributeMountInfo(const matchdoc::MountInfoPtr& singleMountInfo,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
            const std::string &tableName, uint32_t& beginMountId);
    static void InsertPackAttributeMountInfo(const matchdoc::MountInfoPtr& singleMountInfo,
            const IE_NAMESPACE(config)::PackAttributeConfigPtr& packAttrConf,
            const std::string &tableName, uint32_t mountId);
    bool singleJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) override;
    bool multiJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) override;
private:
    std::string _kvTableName;
    IE_NAMESPACE(index)::KVReaderPtr _kvReader;
    ValueCollectorPtr _valueCollectorPtr;
};

HA3_TYPEDEF_PTR(KvLookupJoinKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_KV_LOOK_UP_JOIN_KERNEL_H
