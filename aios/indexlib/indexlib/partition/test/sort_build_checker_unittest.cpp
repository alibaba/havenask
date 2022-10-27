#include <autil/StringUtil.h>
#include "indexlib/partition/test/sort_build_checker_unittest.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SortBuildCheckerTest);

SortBuildCheckerTest::SortBuildCheckerTest()
{
}

SortBuildCheckerTest::~SortBuildCheckerTest()
{
}

void SortBuildCheckerTest::CaseSetUp()
{
    string fields = "attr1:uint32;attr2:uint64;pk:uint32";
    string indexs = "pk_index:primarykey64:pk";
    string attributes = "attr1;attr2";
    mSchema = SchemaMaker::MakeSchema(fields, indexs, attributes, "");

    mPartitionMeta.reset(new PartitionMeta);
    mPartitionMeta->AddSortDescription("attr1", sp_desc);
    mPartitionMeta->AddSortDescription("attr2", sp_asc);
}

void SortBuildCheckerTest::CaseTearDown()
{
}

void SortBuildCheckerTest::TestSimpleProcess()
{
    string docStrs = "cmd=add,pk=pk1,attr1=4,attr2=0;"
                     "cmd=add,pk=pk2,attr1=3,attr2=1;"
                     "cmd=add,pk=pk3,attr1=3,attr2=2;";

    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(mSchema, docStrs);

    CheckSortSequence(docs, "0,0", "");
    CheckSortSequence(docs, "0,1,2", "");
    CheckSortSequence(docs, "1,0", "pk1");
    CheckSortSequence(docs, "2,1", "pk2");
}

void SortBuildCheckerTest::CheckSortSequence(
        const vector<NormalDocumentPtr>& docs,
        const std::string& docIdxs,
        const std::string& failDocPk)
{
    SortBuildChecker checker;
    checker.Init(mSchema->GetAttributeSchema(), *mPartitionMeta);
    
    vector<size_t> posVector;
    StringUtil::fromString(docIdxs, posVector, ",");

    for (size_t i = 0; i < posVector.size(); i++)
    {
        assert(posVector[i] < docs.size());

        NormalDocumentPtr doc = docs[posVector[i]];
        const string& pk = doc->GetIndexDocument()->GetPrimaryKey();
        
        if (pk == failDocPk)
        {
            ASSERT_FALSE(checker.CheckDocument(doc));
            break;
        }
        ASSERT_TRUE(checker.CheckDocument(doc));
    }
}

IE_NAMESPACE_END(partition);

