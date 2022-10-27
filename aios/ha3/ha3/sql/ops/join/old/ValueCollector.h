#ifndef ISEARCH_VALUE_COLLECTOR_H
#define ISEARCH_VALUE_COLLECTOR_H

#include <ha3/sql/common/common.h>
#include <matchdoc/Reference.h>
#include <matchdoc/MatchDocAllocator.h>
#include <indexlib/indexlib.h>
#include <indexlib/index/kkv/kkv_reader.h>
#include <indexlib/config/index_partition_schema.h>
#include <ha3/util/Log.h>


BEGIN_HA3_NAMESPACE(sql);

class ValueCollector {
private:
    typedef std::vector<matchdoc::ReferenceBase*> ValueReferences;

public:
    ValueCollector();
    ~ValueCollector();

public:
    bool init(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& tableSchema,
              const matchdoc::MatchDocAllocatorPtr& allocator);
    // for kkv collect
    void collectValueFields(matchdoc::MatchDoc matchDoc, IE_NAMESPACE(index)::KKVIterator* iter);
    // for kv collect
    void collectValueFields(matchdoc::MatchDoc matchDoc, const autil::ConstString& value);

private:
    matchdoc::ReferenceBase* declareReference(const matchdoc::MatchDocAllocatorPtr& allocator,
                                              FieldType fieldType, bool isMulti,
                                              const std::string& valueName,
                                              const std::string& groupName);
    void setValue(matchdoc::MatchDoc doc, matchdoc::ReferenceBase* ref, const char* value);

private:
    ValueReferences _valueRefs;
    matchdoc::ReferenceBase* _skeyRef;

private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(ValueCollector);

END_HA3_NAMESPACE(sql);

#endif  //ISEARCH_VALUE_COLLECTOR_H
