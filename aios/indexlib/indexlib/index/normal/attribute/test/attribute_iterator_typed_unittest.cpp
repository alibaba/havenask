#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"

using namespace std;
using namespace std::tr1;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class FakeInt32SegmentReader
{
public:
    FakeInt32SegmentReader(const vector<int32_t>& dataVector)
        : mDataVector(dataVector)
    {
    }

    bool Read(docid_t docId, int32_t& value, autil::mem_pool::Pool* pool)
    {
        value = mDataVector[docId];
        return true;
    }
private:
    const vector<int32_t> mDataVector;
};

typedef tr1::shared_ptr<FakeInt32SegmentReader> FakeInt32SegmentReaderPtr;

class FakeInt32BuildingSegReader
{
public:
    FakeInt32BuildingSegReader(const vector<int32_t>& dataVector)
        : mDataVector(dataVector)
    {
    }

    uint64_t GetDocCount() { return mDataVector.size(); }

    bool Read(docid_t docId, int32_t& value, autil::mem_pool::Pool* pool)
    {
        if (docId < (docid_t)mDataVector.size())
        {
            value = mDataVector[docId];
            return true;
        }
        return false;
    }
private:
    const vector<int32_t> mDataVector;
};


typedef tr1::shared_ptr<FakeInt32BuildingSegReader> FakeInt32BuildingSegReaderPtr;

template<typename T>
struct FakeAttributeReaderTraits
{
};

template<>
struct FakeAttributeReaderTraits<int32_t>
{
    typedef FakeInt32SegmentReader SegmentReader;
    typedef FakeInt32BuildingSegReader InMemSegmentReader;
};

class AttributeIteratorTypedTest : public INDEXLIB_TESTBASE
{
public:
    typedef AttributeIteratorTyped<int32_t, FakeAttributeReaderTraits<int32_t> > Int32AttrIterator;
    typedef tr1::shared_ptr<Int32AttrIterator> Int32AttrIteratorPtr;
    
    typedef BuildingAttributeReader<int32_t, FakeAttributeReaderTraits<int32_t> > BuildingAttributeReaderType;
    typedef tr1::shared_ptr<BuildingAttributeReaderType> BuildingAttributeReaderPtr;

    typedef vector<FakeInt32SegmentReaderPtr> SegmentReaderVect;

    void CaseSetUp() override
    {
        static const uint32_t SEED = 8888;
        srand(SEED);
    }

    void CaseTearDown() override
    {
    }

    void TestSeekInOrderForOneSegment()
    {
        vector<uint32_t> docCountPerSegment;
        docCountPerSegment.push_back(10);
        TestSeekInOrder(docCountPerSegment);  // test one segment
    }

    void TestSeekInOrderForMultiSegments()
    {
        vector<uint32_t> docCountPerSegment;
        docCountPerSegment.push_back(10);
        docCountPerSegment.push_back(30);
        docCountPerSegment.push_back(0); // empty segment
        docCountPerSegment.push_back(100);

        TestSeekInOrder(docCountPerSegment);  // test multiple segment
    }

    void TestRandomSeekForOneSegment()
    {
        vector<uint32_t> docCountPerSegment;
        docCountPerSegment.push_back(10);

        vector<docid_t> docIdsToSeek;
        docIdsToSeek.push_back(0);
        docIdsToSeek.push_back(9);
        docIdsToSeek.push_back(5);
        TestRandomSeek(docCountPerSegment, docIdsToSeek);
    }

    void TestRandomSeekForMultiSegments()
    {
        vector<uint32_t> docCountPerSegment;
        docCountPerSegment.push_back(10);
        docCountPerSegment.push_back(30);
        docCountPerSegment.push_back(60);
        uint32_t totalDocCount = GetTotalDocCount(docCountPerSegment);
        
        vector<docid_t> docIdsToSeek;
        docIdsToSeek.push_back(0); // segment 0
        docIdsToSeek.push_back(80); // segment 2
        docIdsToSeek.push_back(20); // segment 1
        docIdsToSeek.push_back(4); // segment 0
        docIdsToSeek.push_back(25); // segment 1
        TestRandomSeek(docCountPerSegment, docIdsToSeek);

        vector<docid_t> docIdsToSeek2;
        static const uint32_t SEEK_TIMES = 100;
        for (uint32_t i = 0; i < SEEK_TIMES; ++i)
        {
            docIdsToSeek2.push_back(rand() % totalDocCount);
        }
        TestRandomSeek(docCountPerSegment, docIdsToSeek2);        
    }

    void TestSeekOutOfRange()
    {
        vector<uint32_t> docCountPerSegment;
        docCountPerSegment.push_back(10);
        docCountPerSegment.push_back(30);
        docCountPerSegment.push_back(60);
//        uint32_t totalDocCount = GetTotalDocCount(docCountPerSegment);
        
        vector<docid_t> docIdsToSeek;
        docIdsToSeek.push_back(300); // segment 0
        docIdsToSeek.push_back(80); // segment 2
        vector<int32_t> dataVector;
        Int32AttrIteratorPtr iterator = PrepareAttrIterator(docCountPerSegment, dataVector);
        int value;
        INDEXLIB_TEST_TRUE(!iterator->Seek(10000, value));
        INDEXLIB_TEST_TRUE(iterator->Seek(1, value));
    }

    void TestSeekWithBuildingReader()
    {
        vector<uint32_t> docCountPerSegment;
        docCountPerSegment.push_back(1);
        docCountPerSegment.push_back(1);
        docCountPerSegment.push_back(2);
        
        vector<int32_t> dataVector;
        Int32AttrIteratorPtr iterator = PrepareAttrIterator(docCountPerSegment, dataVector, true);

        int32_t value;
        
        for (docid_t i = 0; i < 4; i++)
        {
            INDEXLIB_TEST_TRUE(iterator->Seek(i, value));
            INDEXLIB_TEST_EQUAL(dataVector[i], value);
        }

        for (docid_t i = 0; i < 4; i++)
        {
            INDEXLIB_TEST_TRUE(iterator->SeekInRandomMode(i, value));
            INDEXLIB_TEST_EQUAL(dataVector[i], value);
        }

        INDEXLIB_TEST_TRUE(!iterator->SeekInRandomMode(100, value));
    }

private:
    void CreateOneSegmentData(vector<int32_t>& dataVector, uint32_t docCount)
    {
        for (uint32_t i = 0; i < docCount; ++i)
        {
            dataVector.push_back(rand() % 100);
        }
    }

    uint32_t GetTotalDocCount(const vector<uint32_t>& docCountPerSegment)
    {
        uint32_t totalDocCount = 0;
        for (size_t i = 0; i < docCountPerSegment.size(); ++i)
        {
            totalDocCount += docCountPerSegment[i];
        }
        return totalDocCount;
    }

    Int32AttrIteratorPtr PrepareAttrIterator(
            const vector<uint32_t>& docCountPerSegment,
            vector<int32_t>& dataVector,
            bool hasRealtimeReader = false)
    {
        docid_t baseDocId = 0;
        mSegReaders.clear();
        mSegInfos.clear();

        size_t segmentSize = docCountPerSegment.size();
        if (hasRealtimeReader)
        {
            segmentSize =  segmentSize - 1;
        }

        for (size_t i = 0; i < segmentSize; ++i)
        {
            vector<int32_t> segDataVector;
            CreateOneSegmentData(segDataVector, docCountPerSegment[i]);
            dataVector.insert(dataVector.end(), segDataVector.begin(), segDataVector.end());

            FakeInt32SegmentReaderPtr segReader(new FakeInt32SegmentReader(segDataVector));
            mSegReaders.push_back(segReader);

            SegmentInfo segInfo;
            segInfo.docCount = docCountPerSegment[i];
            mSegInfos.push_back(segInfo);
            baseDocId += docCountPerSegment[i];
        }

        BuildingAttributeReaderPtr buildingAttrReader;
        if (hasRealtimeReader)
        {
            vector<int32_t> segDataVector;
            CreateOneSegmentData(segDataVector, docCountPerSegment[segmentSize]);
            dataVector.insert(dataVector.end(), segDataVector.begin(), segDataVector.end());
            FakeInt32BuildingSegReaderPtr buildingReader(
                    new FakeInt32BuildingSegReader(segDataVector));
            buildingAttrReader.reset(new BuildingAttributeReaderType);
            buildingAttrReader->AddSegmentReader(baseDocId, buildingReader);
        }

        Int32AttrIterator* attrIt = new Int32AttrIterator(
                mSegReaders, buildingAttrReader, mSegInfos, INVALID_DOCID, NULL);
        return Int32AttrIteratorPtr(attrIt);
    }

    void TestSeekInOrder(const vector<uint32_t>& docCountPerSegment)
    {
        vector<int32_t> dataVector;
        Int32AttrIteratorPtr iterator = PrepareAttrIterator(docCountPerSegment, dataVector);
        CheckSequentialSeek(iterator, dataVector);
    }

    void TestRandomSeek(const vector<uint32_t>& docCountPerSegment, 
                        vector<docid_t>& docIdsToSeek)
    {
        vector<int32_t> dataVector;
        Int32AttrIteratorPtr iterator = PrepareAttrIterator(docCountPerSegment, dataVector);
        CheckRandomSeek(iterator, dataVector, docIdsToSeek);
    }

    void CheckSequentialSeek(Int32AttrIteratorPtr& iterator,
                             const vector<int32_t>& expectedData)
    {
        for (size_t i = 0; i < expectedData.size(); ++i)
        {
            int32_t value;
            INDEXLIB_TEST_TRUE(iterator->Seek((docid_t)i, value));
            assert(expectedData[i] == value);
            INDEXLIB_TEST_EQUAL(expectedData[i], value);
        }
    }

    void CheckRandomSeek(Int32AttrIteratorPtr& iterator,
                         const vector<int32_t>& expectedData, 
                         const vector<docid_t>& docIdsToSeek)
    {
        for (size_t i = 0; i < docIdsToSeek.size(); ++i)
        {
            int32_t value;
            INDEXLIB_TEST_TRUE(iterator->Seek((docid_t)i, value));
            assert(expectedData[i] == value);
            INDEXLIB_TEST_EQUAL(expectedData[i], value);
        }
    }

private:
    SegmentReaderVect mSegReaders;
    SegmentInfos mSegInfos;
};

INDEXLIB_UNIT_TEST_CASE(AttributeIteratorTypedTest, TestSeekInOrderForOneSegment);
INDEXLIB_UNIT_TEST_CASE(AttributeIteratorTypedTest, TestSeekInOrderForMultiSegments);
INDEXLIB_UNIT_TEST_CASE(AttributeIteratorTypedTest, TestRandomSeekForOneSegment);
INDEXLIB_UNIT_TEST_CASE(AttributeIteratorTypedTest, TestRandomSeekForMultiSegments);
INDEXLIB_UNIT_TEST_CASE(AttributeIteratorTypedTest, TestSeekOutOfRange);
INDEXLIB_UNIT_TEST_CASE(AttributeIteratorTypedTest, TestSeekWithBuildingReader);

IE_NAMESPACE_END(index);
