#ifndef ISEARCH_SQL_KEY_VALUE_COLLECTOR_H
#define ISEARCH_SQL_KEY_VALUE_COLLECTOR_H

#include <tr1/memory>
#include <matchdoc/Reference.h>
#include <matchdoc/MatchDocAllocator.h>
#include <indexlib/indexlib.h>
#include <indexlib/config/index_partition_schema.h>
#include <ha3/common.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(sql);

////////////////////////////////////////////////////////////////////////
class KeyCollector {
public:
    KeyCollector();
    ~KeyCollector();
public:
    bool init(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& tableSchema,
              const matchdoc::MatchDocAllocatorPtr& mountedAllocator);
    matchdoc::BuiltinType getPkeyBuiltinType() const {
        return _pkeyBuiltinType;
    }
    matchdoc::ReferenceBase* getPkeyRef() {
        return _pkeyRef;
    }
private:
    matchdoc::BuiltinType _pkeyBuiltinType;
    matchdoc::ReferenceBase* _pkeyRef;
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(KeyCollector);

////////////////////////////////////////////////////////////////////////
class ValueCollector {
private:
    typedef std::vector<matchdoc::ReferenceBase*> ValueReferences;
public:
    ValueCollector();
    ~ValueCollector();
public:
    bool init(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& tableSchema,
              const matchdoc::MatchDocAllocatorPtr& mountedAllocator);
    // for kv collect
    void collectFields(matchdoc::MatchDoc matchDoc,
                       const autil::ConstString& value);
private:
    void setValue(matchdoc::MatchDoc doc, matchdoc::ReferenceBase* ref, const char* value);
private:
    matchdoc::ReferenceBase* _skeyRef;
    ValueReferences _valueRefs;
    matchdoc::Reference<uint64_t>* _tsRef;
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(ValueCollector);

////////////////////////////////////////////////////////////////////////
class CollectorUtil {
public:
    static const std::string MOUNT_TABLE_GROUP;

public:
    static matchdoc::MatchDocAllocatorPtr
    createMountedMatchDocAllocator(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
                                   autil::mem_pool::Pool *pool);

    static matchdoc::ReferenceBase*
    declareReference(const matchdoc::MatchDocAllocatorPtr& mountedAllocator,
                     FieldType fieldType, bool isMulti,
                     const std::string& valueName,
                     const std::string& groupName = MOUNT_TABLE_GROUP);

    static bool isVaildMatchDoc(const matchdoc::MatchDoc &matchDoc) {
        return matchDoc.getDocId() != matchdoc::INVALID_MATCHDOC.getDocId();
    }
public:
    CollectorUtil() {}
    ~CollectorUtil(){}
private:
    CollectorUtil(const CollectorUtil &);
    CollectorUtil &operator=(const CollectorUtil &);

private:
    static matchdoc::MountInfoPtr
    createPackAttributeMountInfo(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema);
    static void insertPackAttributeMountInfo(const matchdoc::MountInfoPtr &singleMountInfo,
                                             const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
                                             const std::string &tableName, uint32_t &beginMountId);
    static void insertPackAttributeMountInfo(const matchdoc::MountInfoPtr &singleMountInfo,
                                             const IE_NAMESPACE(config)::PackAttributeConfigPtr &packAttrConf,
                                             const std::string &tableName, uint32_t mountId);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(sql);

#endif  //ISEARCH_SQL_KEY_VALUE_COLLECTOR_H
