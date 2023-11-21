#pragma once
#include <stdlib.h>

#include "autil/ConstString.h"
#include "autil/LongHashValue.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SingleValueAttributeSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    enum ValueMode { VM_NORMAL, VM_ASC, VM_DESC };
    enum SearchType { ST_LOWERBOUND, ST_UPPERBOUND };

public:
    SingleValueAttributeSegmentReaderTest() {}
    ~SingleValueAttributeSegmentReaderTest() {}

    DECLARE_CLASS_NAME(SingleValueAttributeSegmentReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestIsInMemory();
    void TestGetDataLength();
    void TestCaseForUInt32NoPatch();
    void TestCaseForInt32NoPatch();
    void TestCaseForUInt32Patch();
    void TestCaseForInt32Patch();
    void TestCaseForUInt64NoPatch();
    void TestCaseForInt64NoPatch();
    void TestCaseForUInt64Patch();
    void TestCaseForInt64Patch();
    void TestCaseForUInt8NoPatch();
    void TestCaseForInt8NoPatch();
    void TestCaseForUInt8Patch();
    void TestCaseForInt8Patch();
    void TestCaseForUInt16NoPatch();
    void TestCaseForInt16NoPatch();
    void TestCaseForUInt16Patch();
    void TestCaseForInt16Patch();
    // TODO(xiaohao.yxh) still useful?
    // void TestCaseForUpdateFieldWithoutPatch();
    // void TestCaseForUpdateFieldWithPatch();

    void TestCaseForSearch();
    void TestCaseDoOpenWithoutPatch();

protected:
    virtual merger::SegmentDirectoryPtr CreateSegmentDirectory(const indexlib::file_system::DirectoryPtr& dir,
                                                               uint32_t segCount)
    {
        return IndexTestUtil::CreateSegmentDirectory(dir, segCount);
    }

    template <typename T>
    void TestUpdateFieldWithoutPatch(bool needCompress);

    template <typename T>
    void TestUpdateFieldWithPatch(bool needCompress);

    // template <typename T>
    // void UpdateAndCheck(uint32_t segCount, std::vector<T>& expectedData);

    template <typename T>
    void MakeUpdateData(std::map<docid_t, T>& toUpdateDocs);

    template <typename T>
    void TestOpenForNoPatch();

    template <typename T>
    void TestOpenForPatch();

    template <typename T>
    void TestSearch(bool compress = false, bool needNull = false);

    template <typename T>
    void FullBuild(std::vector<T>& expectedData, ValueMode valueMode = VM_NORMAL, bool needCompress = false,
                   bool needNull = false);

    template <typename T>
    void BuildIndex(const std::vector<T>& data, bool needCompress = false);

    template <typename T, typename Comp>
    void CheckSearch(const std::vector<T>& expectedData, uint32_t segmentCount, ValueMode mode, SearchType type);

    template <typename T>
    void CheckOpen(const std::vector<T>& expectedData, uint32_t segmentCount, bool bWithPatch);

    template <typename T>
    T GetValueByMode(uint32_t i, ValueMode mode);

    template <typename T>
    void CheckRead(const std::vector<T>& expectedData, const SingleValueAttributeSegmentReader<T>& segReader);
    template <typename T>
    void CreatePatches(std::vector<T>& expectedData, uint32_t patchCount);

    template <typename T>
    void CreateOnePatch(std::vector<T>& expectedData, segmentid_t outputSegId);

protected:
    std::string mRoot;
    config::AttributeConfigPtr mAttrConfig;
    file_system::DirectoryPtr mSeg0Directory;

    static const uint32_t DOC_NUM = 100;
    static const uint32_t UPDATE_DOC_NUM = 20;
};

//////////////////////////////////////////////////////////////////////
// template <typename T>
// void SingleValueAttributeSegmentReaderTest::TestUpdateFieldWithoutPatch(bool needCompress)
// {
//     std::vector<T> expectedData;
//     for (uint32_t i = 0; i < DOC_NUM; i++) {
//         expectedData.push_back(i);
//     }

//     BuildIndex(expectedData, needCompress);
//     UpdateAndCheck(1, expectedData);
// }

// template <typename T>
// void SingleValueAttributeSegmentReaderTest::TestUpdateFieldWithPatch(bool needCompress)
// {
//     std::vector<T> expectedData;
//     for (uint32_t i = 0; i < DOC_NUM; i++) {
//         expectedData.push_back(i);
//     }
//     BuildIndex(expectedData, needCompress);
//     uint32_t patchCount = 3;
//     CreatePatches(expectedData, patchCount);
//     UpdateAndCheck(patchCount + 1, expectedData);
// }

// template <typename T>
// void SingleValueAttributeSegmentReaderTest::UpdateAndCheck(uint32_t segCount, std::vector<T>& expectedData)
// {
//     RESET_FILE_SYSTEM();
//     index_base::PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
//     index_base::SegmentData segData = partData->GetSegmentData(0);

//     AttributeSegmentPatchIteratorPtr attrPatchIterator(new SingleValueAttributePatchReader<T>(mAttrConfig));
//     attrPatchIterator->Init(partData, 0);
//     SingleValueAttributeSegmentReader<T> segReader(mAttrConfig);
//     segReader.Open(segData, file_system::DirectoryPtr(), attrPatchIterator);

//     std::map<docid_t, T> toUpdateDocs;
//     MakeUpdateData(toUpdateDocs);

//     // update field
//     typename std::map<docid_t, T>::iterator it = toUpdateDocs.begin();
//     for (; it != toUpdateDocs.end(); it++) {
//         docid_t docId = it->first;
//         T value = it->second;
//         bool isNull = false;
//         segReader.UpdateField(docId, (uint8_t*)&value, sizeof(value), isNull);
//         expectedData[docId] = value;
//     }

//     CheckRead(expectedData, segReader);
// }

template <typename T>
void SingleValueAttributeSegmentReaderTest::MakeUpdateData(std::map<docid_t, T>& toUpdateDocs)
{
    for (uint32_t i = 0; i < UPDATE_DOC_NUM; i++) {
        docid_t docId = rand() % DOC_NUM;
        toUpdateDocs[docId] = docId + 20;
    }
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::TestOpenForNoPatch()
{
    std::vector<T> expectedData;
    FullBuild(expectedData);
    CheckOpen(expectedData, 1, false);
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::TestOpenForPatch()
{
    std::vector<T> expectedData;
    FullBuild(expectedData);
    CreatePatches(expectedData, 3);
    CheckOpen(expectedData, 4, true);
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::TestSearch(bool compress, bool needNull)
{
    std::vector<T> expectedData;
    FullBuild(expectedData, VM_DESC, compress, needNull);
    CheckSearch<T, std::less<T>>(expectedData, 1, VM_DESC, ST_LOWERBOUND);
    FullBuild(expectedData, VM_DESC, compress, needNull);
    CheckSearch<T, std::less_equal<T>>(expectedData, 1, VM_DESC, ST_UPPERBOUND);
    FullBuild(expectedData, VM_ASC, compress, needNull);
    CheckSearch<T, std::greater<T>>(expectedData, 1, VM_ASC, ST_LOWERBOUND);
    FullBuild(expectedData, VM_ASC, compress, needNull);
    CheckSearch<T, std::greater_equal<T>>(expectedData, 1, VM_ASC, ST_UPPERBOUND);
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::FullBuild(std::vector<T>& expectedData, ValueMode valueMode,
                                                      bool needCompress, bool needNull)
{
    tearDown();
    setUp();

    expectedData.clear();
    mAttrConfig = AttributeTestUtil::CreateAttrConfig<T>();
    if (needCompress) {
        ASSERT_TRUE(mAttrConfig->SetCompressType("equal").IsOK());
    }
    mAttrConfig->GetFieldConfig()->SetEnableNullField(needNull);

    SingleValueAttributeWriter<T> writer(mAttrConfig);
    common::AttributeConvertorFactory* factory = common::AttributeConvertorFactory::GetInstance();
    common::AttributeConvertorPtr convertor(factory->CreateAttrConvertor(mAttrConfig));
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    autil::mem_pool::Pool pool;
    docid_t lastDocId = 0;
    if (needNull && valueMode == VM_ASC) {
        // add some null field at the begin
        for (int32_t i = 0; i < 10; i++) {
            writer.AddField(i, autil::StringView::empty_instance(), true);
        }
        lastDocId = 10;
    }
    for (uint32_t i = 0; i < DOC_NUM; ++i) {
        T value = GetValueByMode<T>(i, valueMode);
        expectedData.push_back(value);

        char buf[64];
        snprintf(buf, sizeof(buf), "%d", (int32_t)value);
        autil::StringView encodeValue = convertor->Encode(autil::StringView(buf), &pool);
        writer.AddField((docid_t)i + lastDocId, encodeValue, false);
    }
    if (needNull && valueMode == VM_DESC) {
        // add some null field at the last
        for (uint32_t i = 0; i < 10; i++) {
            writer.AddField((docid_t)(i + DOC_NUM), autil::StringView::empty_instance(), true);
        }
    }

    file_system::DirectoryPtr attrDirectory = mSeg0Directory->MakeDirectory(ATTRIBUTE_DIR_NAME);
    util::SimplePool dumpPool;
    writer.Dump(attrDirectory, &dumpPool);

    index_base::SegmentInfo segmentInfo;
    segmentInfo.docCount = needNull ? DOC_NUM + 10 : DOC_NUM;
    segmentInfo.Store(mSeg0Directory);
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::BuildIndex(const std::vector<T>& data, bool needCompress)
{
    tearDown();
    setUp();
    mAttrConfig = AttributeTestUtil::CreateAttrConfig<T>();
    if (needCompress) {
        ASSERT_TRUE(mAttrConfig->SetCompressType("equal").IsOK());
    }
    SingleValueAttributeWriter<T> writer(mAttrConfig);
    common::AttributeConvertorFactory* factory = common::AttributeConvertorFactory::GetInstance();
    common::AttributeConvertorPtr convertor(factory->CreateAttrConvertor(mAttrConfig));
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < data.size(); i++) {
        std::string attrValue = autil::StringUtil::toString<T>(data[i]);
        autil::StringView encodeValue = convertor->Encode(autil::StringView(attrValue), &pool);
        writer.AddField((docid_t)i, encodeValue);
    }

    file_system::DirectoryPtr attrDirectory = mSeg0Directory->MakeDirectory(ATTRIBUTE_DIR_NAME);
    util::SimplePool dumpPool;
    writer.Dump(attrDirectory, &dumpPool);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = data.size();
    segInfo.Store(mSeg0Directory);
}

template <typename T, typename Comp>
void SingleValueAttributeSegmentReaderTest::CheckSearch(const std::vector<T>& expectedData, uint32_t segmentCount,
                                                        ValueMode mode, SearchType type)
{
    index_base::PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segmentCount);
    index_base::SegmentData segData = partData->GetSegmentData(0);

    SingleValueAttributeSegmentReader<T> segReader(mAttrConfig);
    segReader.Open(segData, PatchApplyOption::NoPatch(), nullptr, false);
    CheckRead(expectedData, segReader);

    const indexlibv2::config::SortPattern sp =
        mode == VM_DESC ? indexlibv2::config::sp_desc : indexlibv2::config::sp_asc;
    docid_t docCount = segData.GetSegmentInfo()->docCount;
    docid_t nullCount = docCount - expectedData.size();
    DocIdRange rangeLimit(0, docCount);
    ASSERT_EQ(nullCount, segReader.SearchNullCount(sp));

    for (size_t i = 0; i < expectedData.size(); ++i) {
        T value = expectedData[i];
        docid_t docid = INVALID_DOCID;
        segReader.template Search<Comp>(value, rangeLimit, sp, docid);
        if (type == ST_UPPERBOUND) {
            docid_t expectedDocid =
                sp == indexlibv2::config::sp_asc ? (docid_t)i / 2 * 2 + 2 + nullCount : (docid_t)i / 2 * 2 + 2;
            INDEXLIB_TEST_EQUAL(expectedDocid, docid);
        }
        if (type == ST_LOWERBOUND) {
            docid_t expectedDocid =
                sp == indexlibv2::config::sp_asc ? (docid_t)i / 2 * 2 + nullCount : (docid_t)i / 2 * 2;
            INDEXLIB_TEST_EQUAL(expectedDocid, docid);
        }
    }
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::CheckOpen(const std::vector<T>& expectedData, uint32_t segmentCount,
                                                      bool bWithPatch)
{
    RESET_FILE_SYSTEM();
    index_base::PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segmentCount);
    index_base::SegmentData segData = partData->GetSegmentData(0);

    AttributeSegmentPatchIteratorPtr attrPatchIterator;
    if (bWithPatch) {
        attrPatchIterator.reset(new SingleValueAttributePatchReader<T>(mAttrConfig));
        attrPatchIterator->Init(partData, 0);
    }

    SingleValueAttributeSegmentReader<T> segReader(mAttrConfig);
    segReader.Open(segData, PatchApplyOption::OnRead(attrPatchIterator), file_system::DirectoryPtr(), true);
    CheckRead(expectedData, segReader);
}

template <typename T>
T SingleValueAttributeSegmentReaderTest::GetValueByMode(uint32_t i, ValueMode mode)
{
    T value;
    switch (mode) {
    case VM_ASC:
        value = i / 2 * 2;
        break;
    case VM_DESC:
        value = (DOC_NUM - i - 1) / 2 * 2;
        break;
    case VM_NORMAL:
    default:
        value = i * 3 % 10;
        break;
    }
    return value;
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::CheckRead(const std::vector<T>& expectedData,
                                                      const SingleValueAttributeSegmentReader<T>& segReader)
{
    size_t i = 0;
    bool supportNull = mAttrConfig->GetFieldConfig()->IsEnableNullField();
    for (size_t j = 0; j < segReader.mDocCount; j++) {
        T value;
        bool isNull = true;
        auto ctx = segReader.CreateReadContext(nullptr);
        segReader.Read(j, value, isNull, ctx);
        if (!supportNull) {
            ASSERT_FALSE(isNull);
        }
        if (isNull) {
            continue;
        }
        INDEXLIB_TEST_EQUAL(expectedData[i], value);
        i++;
    }
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::CreatePatches(std::vector<T>& expectedData, uint32_t patchCount)
{
    for (uint32_t i = 1; i <= patchCount; i++) {
        CreateOnePatch(expectedData, i);
    }
}

template <typename T>
void SingleValueAttributeSegmentReaderTest::CreateOnePatch(std::vector<T>& expectedData, segmentid_t outputSegId)
{
    std::stringstream ss;
    ss << mRoot << SEGMENT_FILE_NAME_PREFIX << "_" << outputSegId << "_level_0/";
    IndexTestUtil::ResetDir(ss.str());
    index_base::SegmentInfo segmentInfo;
    segmentInfo.TEST_Store(ss.str() + SEGMENT_INFO_FILE_NAME);

    ss << ATTRIBUTE_DIR_NAME << "/";
    IndexTestUtil::ResetDir(ss.str());
    ss << mAttrConfig->GetAttrName() << "/";
    IndexTestUtil::ResetDir(ss.str());
    std::string patchFile = ss.str() + autil::StringUtil::toString<segmentid_t>(outputSegId) + "_0." + PATCH_FILE_NAME;
    std::unique_ptr<fslib::fs::File> file(fslib::fs::FileSystem::openFile(patchFile.c_str(), fslib::WRITE));

    for (uint32_t j = 0; j < DOC_NUM; j++) {
        if ((outputSegId + j) % 10 == 1) {
            T value = (outputSegId + j) % 23;
            expectedData[j] = value;
            file->write((void*)(&j), sizeof(docid_t));
            file->write((void*)(&value), sizeof(T));
        }
    }
    file->close();
}
}} // namespace indexlib::index
