#include "indexlib/index/attribute/AttributeIteratorTyped.h"

#include "unittest/unittest.h"

namespace indexlibv2::index {

class FakeInt32SegmentReader;
typedef int FakeInt32SegmentReadContext;

class FakeInt32SegmentReader
{
public:
    FakeInt32SegmentReader(const std::vector<int32_t>& dataVector) : _dataVector(dataVector) {}
    FakeInt32SegmentReadContext CreateReadContext(autil::mem_pool::Pool* sessionPool) { return 0; }

    bool Read(docid_t docId, int32_t& value, bool& isNull, FakeInt32SegmentReadContext& ctx)
    {
        value = _dataVector[docId];
        return true;
    }
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchRead(const std::vector<docid_t>& docIds,
                                                                     FakeInt32SegmentReadContext& ctx,
                                                                     indexlib::file_system::ReadOption readOption,
                                                                     typename std::vector<int32_t>* values,
                                                                     std::vector<bool>* isNull) const
    {
        values->resize(docIds.size());
        isNull->resize(docIds.size());
        for (size_t i = 0; i < docIds.size(); ++i) {
            docid_t docId = docIds[i];
            (*values)[i] = _dataVector[docId];
            (*isNull)[i] = false;
        }

        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::OK);
    }

private:
    const std::vector<int32_t> _dataVector;
};

class FakeInt32BuildingSegReader
{
public:
    FakeInt32BuildingSegReader(const std::vector<int32_t>& dataVector) : _dataVector(dataVector) {}

    uint64_t GetDocCount() { return _dataVector.size(); }

    bool Read(docid_t docId, int32_t& value, bool& isNull, autil::mem_pool::Pool* pool)
    {
        if (docId < (docid_t)_dataVector.size()) {
            value = _dataVector[docId];
            return true;
        }
        return false;
    }

private:
    const std::vector<int32_t> _dataVector;
};

template <typename T>
struct FakeAttributeReaderTraits {
};

template <>
struct FakeAttributeReaderTraits<int32_t> {
    using SegmentReader = FakeInt32SegmentReader;
    using InMemSegmentReader = FakeInt32BuildingSegReader;
    using SegmentReadContext = FakeInt32SegmentReadContext;
};

class AttributeIteratorTypedTest : public TESTBASE
{
public:
    AttributeIteratorTypedTest();
    ~AttributeIteratorTypedTest();

    using Int32AttrIterator = AttributeIteratorTyped<int32_t, FakeAttributeReaderTraits<int32_t>>;
    using InMemSegmentReader = FakeInt32BuildingSegReader;
    using SegmentReaderVect = std::vector<std::shared_ptr<FakeInt32SegmentReader>>;

public:
    void setUp() override;
    void tearDown() override;

private:
    void CreateOneSegmentData(std::vector<int32_t>& dataVector, uint32_t docCount);
    uint32_t GetTotalDocCount(const std::vector<uint32_t>& docCountPerSegment);
    std::shared_ptr<Int32AttrIterator> PrepareAttrIterator(const std::vector<uint32_t>& docCountPerSegment,
                                                           std::vector<int32_t>& dataVector, bool hasRealtimeReader,
                                                           bool hasDefaultValueReader);
    void TestSeekInOrder(const std::vector<uint32_t>& docCountPerSegment, bool isBatchMode, bool hasDefaultValueReader);
    void TestRandomSeek(const std::vector<uint32_t>& docCountPerSegment, std::vector<docid_t>& docIdsToSeek,
                        bool hasDefaultValueReader);
    void CheckSequentialSeek(std::shared_ptr<Int32AttrIterator>& iterator, const std::vector<int32_t>& expectedData,
                             uint32_t totalDocCount, bool isBatchMode, bool hasDefaultValueReader);
    void CheckRandomSeek(std::shared_ptr<Int32AttrIterator>& iterator, const std::vector<int32_t>& expectedData,
                         const std::vector<docid_t>& docIdsToSeek, uint32_t totalDocCount, bool hasDefaultValueReader);

    template <typename T, FieldType fieldType>
    void InnerTestSingleDefaultValueReaderIter(std::string valueStr, std::optional<T> expectedValue);

    template <typename T, FieldType fieldType>
    void InnerTestMultiDefaultValueReaderIter(std::vector<std::string> valueStrs, const std::vector<T>& expectedValue,
                                              std::optional<size_t> fixedCount);

private:
    const int32_t DEFAULT_VALUE = -1;
    SegmentReaderVect _segReaders;
    std::vector<std::pair<docid_t, std::shared_ptr<InMemSegmentReader>>> buildingAttrReader;
    std::vector<uint64_t> _segDocCount;
};

AttributeIteratorTypedTest::AttributeIteratorTypedTest() {}

AttributeIteratorTypedTest::~AttributeIteratorTypedTest() {}

void AttributeIteratorTypedTest::setUp()
{
    static const uint32_t SEED = 8888;
    srand(SEED);
}

void AttributeIteratorTypedTest::tearDown() {}

TEST_F(AttributeIteratorTypedTest, TestBatchSeek)
{
    std::vector<uint32_t> docCountPerSegment;
    docCountPerSegment.push_back(10);
    docCountPerSegment.push_back(10);
    docCountPerSegment.push_back(10);
    TestSeekInOrder(docCountPerSegment, true, false); // test 3 segment
    TestSeekInOrder(docCountPerSegment, true, true);  // test 3 segment
}

TEST_F(AttributeIteratorTypedTest, TestSeekInOrderForOneSegment)
{
    std::vector<uint32_t> docCountPerSegment;
    docCountPerSegment.push_back(10);
    TestSeekInOrder(docCountPerSegment, false, false); // test one segment
    TestSeekInOrder(docCountPerSegment, false, true);  // test one segment
}

TEST_F(AttributeIteratorTypedTest, TestSeekInOrderForMultiSegments)
{
    std::vector<uint32_t> docCountPerSegment;
    docCountPerSegment.push_back(10);
    docCountPerSegment.push_back(30);
    docCountPerSegment.push_back(0); // empty segment
    docCountPerSegment.push_back(100);

    TestSeekInOrder(docCountPerSegment, false, false); // test multiple segment
    TestSeekInOrder(docCountPerSegment, false, true);  // test one segment
}

TEST_F(AttributeIteratorTypedTest, TestRandomSeekForOneSegment)
{
    std::vector<uint32_t> docCountPerSegment;
    docCountPerSegment.push_back(10);

    std::vector<docid_t> docIdsToSeek;
    docIdsToSeek.push_back(0);
    docIdsToSeek.push_back(9);
    docIdsToSeek.push_back(5);
    TestRandomSeek(docCountPerSegment, docIdsToSeek, true);
    TestRandomSeek(docCountPerSegment, docIdsToSeek, false);
}

TEST_F(AttributeIteratorTypedTest, TestRandomSeekForMultiSegments)
{
    std::vector<uint32_t> docCountPerSegment;
    docCountPerSegment.push_back(10);
    docCountPerSegment.push_back(30);
    docCountPerSegment.push_back(60);
    uint32_t totalDocCount = GetTotalDocCount(docCountPerSegment);

    std::vector<docid_t> docIdsToSeek;
    docIdsToSeek.push_back(0);  // segment 0
    docIdsToSeek.push_back(80); // segment 2
    docIdsToSeek.push_back(20); // segment 1
    docIdsToSeek.push_back(4);  // segment 0
    docIdsToSeek.push_back(25); // segment 1
    TestRandomSeek(docCountPerSegment, docIdsToSeek, true);
    TestRandomSeek(docCountPerSegment, docIdsToSeek, false);

    std::vector<docid_t> docIdsToSeek2;
    static const uint32_t SEEK_TIMES = 100;
    for (uint32_t i = 0; i < SEEK_TIMES; ++i) {
        docIdsToSeek2.push_back(rand() % totalDocCount);
    }
    TestRandomSeek(docCountPerSegment, docIdsToSeek2, true);
    TestRandomSeek(docCountPerSegment, docIdsToSeek2, false);
}

TEST_F(AttributeIteratorTypedTest, TestSeekOutOfRange)
{
    std::vector<uint32_t> docCountPerSegment;
    docCountPerSegment.push_back(10);
    docCountPerSegment.push_back(30);
    docCountPerSegment.push_back(60);

    std::vector<docid_t> docIdsToSeek;
    docIdsToSeek.push_back(300); // segment 0
    docIdsToSeek.push_back(80);  // segment 2
    std::vector<int32_t> dataVector;
    std::shared_ptr<Int32AttrIterator> iterator = PrepareAttrIterator(docCountPerSegment, dataVector, false, false);
    int value;
    ASSERT_TRUE(!iterator->Seek(10000, value));
    ASSERT_TRUE(iterator->Seek(1, value));

    // default reader will return default value
    iterator = PrepareAttrIterator(docCountPerSegment, dataVector, false, true);
    ASSERT_TRUE(iterator->Seek(10000, value));
    ASSERT_EQ(-1, value);
    ASSERT_TRUE(iterator->Seek(1, value));
}

TEST_F(AttributeIteratorTypedTest, TestSeekWithBuildingReader)
{
    std::vector<uint32_t> docCountPerSegment;
    docCountPerSegment.push_back(1);
    docCountPerSegment.push_back(1);
    docCountPerSegment.push_back(2);

    std::vector<int32_t> dataVector;
    std::shared_ptr<Int32AttrIterator> iterator = PrepareAttrIterator(docCountPerSegment, dataVector, true, false);

    int32_t value;

    for (docid_t i = 0; i < 4; i++) {
        ASSERT_TRUE(iterator->Seek(i, value));
        ASSERT_EQ(dataVector[i], value);
    }

    bool isNull = false;
    for (docid_t i = 0; i < 4; i++) {
        ASSERT_TRUE(iterator->SeekInRandomMode(i, value, isNull));
        ASSERT_EQ(dataVector[i], value);
    }
    ASSERT_TRUE(!iterator->SeekInRandomMode(100, value, isNull));

    iterator = PrepareAttrIterator(docCountPerSegment, dataVector, true, true);
    ASSERT_TRUE(iterator->SeekInRandomMode(100, value, isNull));
}

TEST_F(AttributeIteratorTypedTest, TestForSingleDefaultValueReaderIter)
{
    InnerTestSingleDefaultValueReaderIter<int8_t, ft_int8>("-12", -12);
    InnerTestSingleDefaultValueReaderIter<int8_t, ft_int8>("-120000", std::nullopt);
    InnerTestSingleDefaultValueReaderIter<int32_t, ft_int32>("8423", 8423);
    InnerTestSingleDefaultValueReaderIter<int64_t, ft_int64>("100000", 100000);
    InnerTestSingleDefaultValueReaderIter<float, ft_float>("12.322", 12.322);
    InnerTestSingleDefaultValueReaderIter<double, ft_double>("100032.421123", 100032.421123);

    InnerTestSingleDefaultValueReaderIter<uint8_t, ft_uint8>("-12", std::nullopt);
    InnerTestSingleDefaultValueReaderIter<uint32_t, ft_uint32>("8423", 8423);
    InnerTestSingleDefaultValueReaderIter<uint64_t, ft_uint64>("3123", 3123);

    InnerTestSingleDefaultValueReaderIter<uint32_t, ft_time>("02:13:14.326", 7994326);
    InnerTestSingleDefaultValueReaderIter<uint32_t, ft_date>("1992-02-22", 8087);
    InnerTestSingleDefaultValueReaderIter<uint64_t, ft_timestamp>("1991-02-03 13:00:00.001", 665586000001);
}

TEST_F(AttributeIteratorTypedTest, TestForMultiDefaultValueReaderIter)
{
    InnerTestMultiDefaultValueReaderIter<int8_t, ft_int8>({"-12"}, std::vector<int8_t> {-12}, std::nullopt);
    InnerTestMultiDefaultValueReaderIter<int8_t, ft_int8>({"-12", "-100"}, std::vector<int8_t> {-12, -100}, 2);
    InnerTestMultiDefaultValueReaderIter<int32_t, ft_int32>({"212", "333"}, std::vector<int32_t> {212, 333},
                                                            std::nullopt);
    InnerTestMultiDefaultValueReaderIter<int64_t, ft_int64>(
        {"212777", "-12345566", "123"}, std::vector<int64_t> {212777, -12345566, 123}, std::nullopt);

    InnerTestMultiDefaultValueReaderIter<uint8_t, ft_uint8>({"23"}, std::vector<uint8_t> {23}, std::nullopt);
    InnerTestMultiDefaultValueReaderIter<uint32_t, ft_uint32>({"1", "100"}, std::vector<uint32_t> {1, 100},
                                                              std::nullopt);
    InnerTestMultiDefaultValueReaderIter<uint64_t, ft_uint64>({"212777", "23333"},
                                                              std::vector<uint64_t> {212777, 23333}, std::nullopt);

    InnerTestMultiDefaultValueReaderIter<float, ft_float>({"23.00"}, std::vector<float> {23.00}, std::nullopt);
    InnerTestMultiDefaultValueReaderIter<double, ft_double>({"1.123", "3.13"}, std::vector<double> {1.123, 3.13},
                                                            std::nullopt);
}

template <typename T, FieldType fieldType>
void AttributeIteratorTypedTest::InnerTestSingleDefaultValueReaderIter(std::string valueStr,
                                                                       std::optional<T> expectedValue)
{
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", fieldType, false);
    fieldConfig->SetDefaultValue(valueStr);
    auto attrConfig = std::make_shared<AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
    std::shared_ptr<DefaultValueAttributeMemReader> defaultReader(new DefaultValueAttributeMemReader());
    auto ret = defaultReader->Open(attrConfig);
    if (expectedValue) {
        ASSERT_TRUE(ret.IsOK());
    } else {
        ASSERT_FALSE(ret.IsOK());
        return;
    }
    autil::mem_pool::Pool pool;
    std::vector<std::shared_ptr<typename AttributeIteratorTyped<T>::SegmentReader>> segmentReader;
    std::vector<std::pair<docid_t, std::shared_ptr<typename AttributeIteratorTyped<T>::InMemSegmentReader>>>
        buildingReader;
    std::unique_ptr<AttributeIteratorTyped<T>> iter(
        new AttributeIteratorTyped<T>(segmentReader, buildingReader, defaultReader, {0}, nullptr, &pool));
    std::vector<docid_t> checkDocs = {0, 100, 1, 100000};
    for (docid_t doc : checkDocs) {
        T value;
        bool isNull = true;
        ASSERT_TRUE(iter->Seek(doc, value, isNull));
        ASSERT_EQ(expectedValue, value);
        ASSERT_FALSE(isNull);
    }
}

template <typename T, FieldType fieldType>
void AttributeIteratorTypedTest::InnerTestMultiDefaultValueReaderIter(std::vector<std::string> valueStrs,
                                                                      const std::vector<T>& expectedValue,
                                                                      std::optional<size_t> fixedCount)

{
    std::string defaultValue;
    if (!valueStrs.empty()) {
        for (auto valueStr : valueStrs) {
            defaultValue += valueStr + INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
        }
        defaultValue.pop_back();
    }
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", fieldType, true);
    fieldConfig->SetDefaultValue(defaultValue);
    if (fixedCount != std::nullopt) {
        fieldConfig->SetFixedMultiValueCount(fixedCount.value());
    }
    std::shared_ptr<DefaultValueAttributeMemReader> defaultReader(new DefaultValueAttributeMemReader());
    auto attrConfig = std::make_shared<AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
    auto ret = defaultReader->Open(attrConfig);
    ASSERT_TRUE(ret.IsOK());
    autil::mem_pool::Pool pool;
    std::vector<std::shared_ptr<typename AttributeIteratorTyped<autil::MultiValueType<T>>::SegmentReader>>
        segmentReader;
    std::vector<std::pair<
        docid_t, std::shared_ptr<typename AttributeIteratorTyped<autil::MultiValueType<T>>::InMemSegmentReader>>>
        buildingReader;
    std::unique_ptr<AttributeIteratorTyped<autil::MultiValueType<T>>> iter(
        new AttributeIteratorTyped<autil::MultiValueType<T>>(segmentReader, buildingReader, defaultReader, {0}, nullptr,
                                                             &pool));

    std::vector<docid_t> checkDocs = {0, 100, 1, 100000};
    for (docid_t doc : checkDocs) {
        autil::MultiValueType<T> multiValue;
        bool isNull = true;
        ASSERT_TRUE(iter->Seek(doc, multiValue, isNull));
        std::vector<T> values;
        for (size_t i = 0; i < multiValue.size(); ++i) {
            values.push_back(multiValue[i]);
        }
        ASSERT_EQ(expectedValue, values);
        ASSERT_FALSE(isNull);
    }
}

void AttributeIteratorTypedTest::CreateOneSegmentData(std::vector<int32_t>& dataVector, uint32_t docCount)
{
    for (uint32_t i = 0; i < docCount; ++i) {
        auto result = rand() % 100;
        dataVector.push_back(result);
    }
}

uint32_t AttributeIteratorTypedTest::GetTotalDocCount(const std::vector<uint32_t>& docCountPerSegment)
{
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < docCountPerSegment.size(); ++i) {
        totalDocCount += docCountPerSegment[i];
    }
    return totalDocCount;
}

std::shared_ptr<AttributeIteratorTypedTest::Int32AttrIterator>
AttributeIteratorTypedTest::PrepareAttrIterator(const std::vector<uint32_t>& docCountPerSegment,
                                                std::vector<int32_t>& dataVector, bool hasRealtimeReader,
                                                bool hasDefaultValueReader)
{
    docid_t baseDocId = 0;
    _segReaders.clear();
    _segDocCount.clear();
    buildingAttrReader.clear();

    size_t segmentSize = docCountPerSegment.size();
    if (hasRealtimeReader) {
        segmentSize = segmentSize - 1;
    }

    for (size_t i = 0; i < segmentSize; ++i) {
        std::vector<int32_t> segDataVector;
        CreateOneSegmentData(segDataVector, docCountPerSegment[i]);
        dataVector.insert(dataVector.end(), segDataVector.begin(), segDataVector.end());

        std::shared_ptr<FakeInt32SegmentReader> segReader(new FakeInt32SegmentReader(segDataVector));
        _segReaders.push_back(segReader);

        _segDocCount.push_back(docCountPerSegment[i]);
        baseDocId += docCountPerSegment[i];
    }

    if (hasRealtimeReader) {
        std::vector<int32_t> segDataVector;
        CreateOneSegmentData(segDataVector, docCountPerSegment[segmentSize]);
        dataVector.insert(dataVector.end(), segDataVector.begin(), segDataVector.end());
        auto buildingReader = std::make_shared<FakeInt32BuildingSegReader>(segDataVector);
        buildingAttrReader.push_back(std::make_pair(baseDocId, buildingReader));
    }

    std::shared_ptr<DefaultValueAttributeMemReader> defaultReader;
    if (hasDefaultValueReader) {
        auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", ft_int32, false);
        fieldConfig->SetDefaultValue(std::to_string(DEFAULT_VALUE));
        auto attrConfig = std::make_shared<AttributeConfig>();
        auto status = attrConfig->Init(fieldConfig);
        if (!status.IsOK()) {
            assert(false);
            return nullptr;
        }
        defaultReader.reset(new DefaultValueAttributeMemReader());
        auto ret = defaultReader->Open(attrConfig);
        assert(ret.IsOK());
    }
    Int32AttrIterator* attrIt =
        new Int32AttrIterator(_segReaders, buildingAttrReader, defaultReader, _segDocCount, nullptr, nullptr);
    return std::shared_ptr<Int32AttrIterator>(attrIt);
}

void AttributeIteratorTypedTest::TestSeekInOrder(const std::vector<uint32_t>& docCountPerSegment, bool isBatchMode,
                                                 bool hasDefaultValueReader)
{
    std::vector<int32_t> dataVector;
    std::shared_ptr<Int32AttrIterator> iterator =
        PrepareAttrIterator(docCountPerSegment, dataVector, false, hasDefaultValueReader);

    uint32_t totalDocCount = GetTotalDocCount(docCountPerSegment);
    CheckSequentialSeek(iterator, dataVector, totalDocCount, isBatchMode, hasDefaultValueReader);
}

void AttributeIteratorTypedTest::TestRandomSeek(const std::vector<uint32_t>& docCountPerSegment,
                                                std::vector<docid_t>& docIdsToSeek, bool hasDefaultValueReader)
{
    std::vector<int32_t> dataVector;
    std::shared_ptr<Int32AttrIterator> iterator =
        PrepareAttrIterator(docCountPerSegment, dataVector, false, hasDefaultValueReader);
    uint32_t totalDocCount = GetTotalDocCount(docCountPerSegment);
    CheckRandomSeek(iterator, dataVector, docIdsToSeek, totalDocCount, hasDefaultValueReader);
}

void AttributeIteratorTypedTest::CheckSequentialSeek(std::shared_ptr<Int32AttrIterator>& iterator,
                                                     const std::vector<int32_t>& expectedData, uint32_t totalDocCount,
                                                     bool isBatchMode, bool hasDefaultValueReader)
{
    std::vector<int32_t> values;
    if (!isBatchMode) {
        for (size_t i = 0; i < expectedData.size(); ++i) {
            int32_t value;
            ASSERT_TRUE(iterator->Seek((docid_t)i, value));
            assert(expectedData[i] == value);
            ASSERT_EQ(expectedData[i], value);
        }
    } else {
        std::vector<docid_t> docIds;

        for (size_t i = 0; i < expectedData.size(); ++i) {
            docIds.push_back((docid_t)i);
        }
        std::vector<bool> nullValues;
        auto result = future_lite::coro::syncAwait(
            iterator->BatchSeek(docIds, indexlib::file_system::ReadOption(), &values, &nullValues));
        ASSERT_EQ(expectedData.size(), result.size());
        ASSERT_EQ(expectedData.size(), values.size());
        ASSERT_EQ(expectedData.size(), nullValues.size());
        for (size_t i = 0; i < expectedData.size(); ++i) {
            ASSERT_FALSE(nullValues[i]);
            ASSERT_EQ(indexlib::index::ErrorCode::OK, result[i]);
            ASSERT_EQ(expectedData[i], values[i]) << "check " << i << " doc failed";
        }
    }

    if (hasDefaultValueReader) {
        int32_t value;
        ASSERT_TRUE(iterator->Seek((docid_t)(totalDocCount), value));
        ASSERT_EQ(DEFAULT_VALUE, value);
    } else {
        int32_t value;
        ASSERT_FALSE(iterator->Seek((docid_t)(totalDocCount), value));
    }
}

void AttributeIteratorTypedTest::CheckRandomSeek(std::shared_ptr<Int32AttrIterator>& iterator,
                                                 const std::vector<int32_t>& expectedData,
                                                 const std::vector<docid_t>& docIdsToSeek, uint32_t totalDocCount,
                                                 bool hasDefaultValueReader)
{
    for (size_t i = 0; i < docIdsToSeek.size(); ++i) {
        int32_t value;
        ASSERT_TRUE(iterator->Seek((docid_t)i, value));
        assert(expectedData[i] == value);
        ASSERT_EQ(expectedData[i], value);
    }

    if (hasDefaultValueReader) {
        int32_t value;
        ASSERT_TRUE(iterator->Seek((docid_t)(totalDocCount), value));
        ASSERT_EQ(DEFAULT_VALUE, value);
    } else {
        int32_t value;
        ASSERT_FALSE(iterator->Seek((docid_t)(totalDocCount), value));
    }
}

} // namespace indexlibv2::index
