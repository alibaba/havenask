#include <autil/LongHashValue.h>
#include "indexlib/partition/operation_queue/test/remove_operation_creator_unittest.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/util/hash_string.h"
#include "indexlib/partition/operation_queue/remove_operation.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, RemoveOperationCreatorTest);

RemoveOperationCreatorTest::RemoveOperationCreatorTest()
{
}

RemoveOperationCreatorTest::~RemoveOperationCreatorTest()
{
}

void RemoveOperationCreatorTest::CaseSetUp()
{
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            "title:text;price:int32;sale:int32;nid:int64", // this is field schema
            "nid:primarykey64:nid;title:text:title", // this is index schema
            "price;sale;nid", // this is attribute schema
            "" );// this is summary schema

    string docStr = "{ 0,"
                    "[price: (3)],"                                     \
                    "[nid: (1)],"                                       \
                    "[sale: (10)]"                                      \
                    "}";

    DocumentMaker::MockIndexPart answer;
    mDoc = DocumentMaker::MakeDocument(0, docStr, mSchema, answer);
    mDoc->SetTimestamp(10);
}

void RemoveOperationCreatorTest::CaseTearDown()
{
}

void RemoveOperationCreatorTest::TestCreate()
{
    RemoveOperationCreator creator(mSchema);
    OperationBase* operation = creator.Create(mDoc, &mPool);
    RemoveOperation<uint64_t>* removeOperation = 
        dynamic_cast<RemoveOperation<uint64_t>*>(operation);
    ASSERT_TRUE(removeOperation);

    HashString hasher;
    uint64_t hashValue;
    const IndexDocumentPtr &indexDoc = mDoc->GetIndexDocument();
    const string &pkString = indexDoc->GetPrimaryKey();
    hasher.Hash(hashValue, pkString.c_str(), pkString.size());

    ASSERT_EQ(hashValue, removeOperation->mPkHash);
    ASSERT_EQ((int64_t)10, removeOperation->mTimestamp);
}

IE_NAMESPACE_END(partition);

