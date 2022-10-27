#ifndef ISEARCH_BUILDERTESTHELPER_H
#define ISEARCH_BUILDERTESTHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/config/index_partition_schema.h>
#include <build_service/document/RawDocument.h>
#include <build_service/document/ExtendDocument.h>

namespace build_service {
namespace processor {

class BuilderTestHelper
{
private:
    typedef IE_NAMESPACE(config)::IndexPartitionSchema IndexPartitionSchema;
    typedef IE_NAMESPACE(config)::IndexPartitionSchemaPtr IndexPartitionSchemaPtr;
public:
    BuilderTestHelper();
    ~BuilderTestHelper();
private:
    BuilderTestHelper(const BuilderTestHelper &);
    BuilderTestHelper& operator=(const BuilderTestHelper &);
public:
    // format:
    //  key1=value;key2=value2;key3=value3...
    static document::RawDocumentPtr makeRawDocument(const std::string &kvStrs);
    static document::ExtendDocumentPtr makeDocument(const std::string &kvStrs);

    // format:
    // key1=value1;key2=value2;key3=value3..
    static void prepareKeyValueMap(const std::string &inputStr, KeyValueMap &kvMap);

    // fieldConfig format:
    //    fieldName1:fieldType;fieldName2:fieldType...
    // indexConfig format:
    //    indexName1:indexType:fieldName1,fieldName2;indexName2:indexType:fieldName...
    // attributeConfig format:
    //    fieldName1;fieldName2;fieldName3...
    // summaryConfig format:
    //    fieldName1;fieldName2;fieldName3...
    static IndexPartitionSchemaPtr makeSchema(const std::string &fieldConfigs, 
            const std::string &indexConfigs, 
            const std::string &attributeConfigs, 
            const std::string &summaryConfigs);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BuilderTestHelper);

};
};

#endif //ISEARCH_BUILDERTESTHELPER_H
