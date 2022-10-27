#include "indexlib/index/normal/attribute/test/join_docid_attribute_reader_unittest.h"
  
using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, JoinDocidAttributeReaderTest);

JoinDocidAttributeReaderTest::JoinDocidAttributeReaderTest()
{
}

JoinDocidAttributeReaderTest::~JoinDocidAttributeReaderTest()
{
}

void JoinDocidAttributeReaderTest::CaseSetUp()
{
    mMainRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("main")->GetPath();
    mSubRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("sub")->GetPath();
    mPool = new autil::mem_pool::Pool;
    mUseIter = false;
}

void JoinDocidAttributeReaderTest::CaseTearDown()
{
    mMainProvider.reset();
    mSubProvider.reset();
    mReader.reset();
    mIter->~JoinDocidAttributeIterator();
    delete mPool;
}

void JoinDocidAttributeReaderTest::TestSimpleProcess()
{
    mReader = CreateReader("2#1,2", "1", "1,2#3,4", "1");
    mIter = dynamic_cast<JoinDocidAttributeIterator*>(
            mReader->CreateIterator(mPool));
    mUseIter = GET_CASE_PARAM();

    //seq
    ASSERT_EQ(2, GetDocId(0));
    ASSERT_EQ(3, GetDocId(1));
    ASSERT_EQ(4, GetDocId(2));
    ASSERT_EQ(5, GetDocId(3));

    //random
    ASSERT_EQ(4, GetDocId(2));
    ASSERT_EQ(2, GetDocId(0));
    ASSERT_EQ(5, GetDocId(3));
    ASSERT_EQ(3, GetDocId(1));
}

void JoinDocidAttributeReaderTest::TestReadOverflow()
{
    mReader = CreateReader("2", "2,3", "1,2", "1,2,3");
    mIter = dynamic_cast<JoinDocidAttributeIterator*>(
            mReader->CreateIterator(mPool));
    mUseIter = GET_CASE_PARAM();

    ASSERT_EQ(2, GetDocId(0));
    ASSERT_EQ(4, GetDocId(1));
    ASSERT_EQ(5, GetDocId(2));
    ASSERT_EQ(INVALID_DOCID, GetDocId(3));
}

JoinDocidAttributeReaderPtr JoinDocidAttributeReaderTest::CreateReader(
        std::string mainInc, std::string mainRt, std::string subInc, std::string subRt)
{
    mMainProvider.reset(new SingleFieldPartitionDataProvider);
    mMainProvider->Init(mMainRoot, "int32", SFP_ATTRIBUTE);
    mMainProvider->Build(mainInc, SFP_OFFLINE);
    mMainProvider->Build(mainRt, SFP_REALTIME);
    PartitionDataPtr partData = mMainProvider->GetPartitionData();
    AttributeConfigPtr attrConfig = mMainProvider->GetAttributeConfig();
    JoinDocidAttributeReaderPtr reader(new JoinDocidAttributeReader);
    reader->Open(attrConfig, partData);

    mSubProvider.reset(new SingleFieldPartitionDataProvider);
    mSubProvider->Init(mSubRoot, "int32", SFP_ATTRIBUTE);
    mSubProvider->Build(subInc, SFP_OFFLINE);
    mSubProvider->Build(subRt, SFP_REALTIME);
    PartitionDataPtr subPartData = mSubProvider->GetPartitionData();
    reader->InitJoinBaseDocId(subPartData);

    return reader;
}

docid_t JoinDocidAttributeReaderTest::GetDocId(docid_t docId) const
{
    if (!mUseIter)
    {
        return mReader->GetJoinDocId(docId);
    }
    docid_t value = MAX_DOCID;
    bool ret = mIter->Seek(docId, value);
    if (!ret)
    {
        //TODO: judge return false or value == INVALID_DOCID
        assert(value != INVALID_DOCID);
        return INVALID_DOCID;
    }
    return value;
}

IE_NAMESPACE_END(index);

