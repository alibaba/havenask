#pragma once

#include <memory>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using testing::_;
using testing::DoAll;
using testing::NiceMock;
using testing::NotNull;
using testing::Return;
using testing::SetArgReferee;
using testing::SetArrayArgument;

namespace indexlib { namespace index {

class MockUpdatableVarNumAttributeSegmentReader : public AttributeSegmentReader
{
public:
    MOCK_METHOD(void, Open, (const std::string& dir, const index_base::SegmentInfo& segInfo), (override));
    MOCK_METHOD(bool, Read, (docid_t docId, uint8_t* buf, uint32_t bufLen, bool& isNull), (override));
    MOCK_METHOD(bool, ReadDataAndLen, (docid_t docId, uint8_t*& buf, uint32_t bufLen, uint32_t dataLen), (override));
    MOCK_METHOD(void, SetBufferSize, (uint32_t bufSize), (override));
    MOCK_METHOD(bool, IsInMemory, (), (const, override));
    MOCK_METHOD(uint32_t, GetDataLength, (docid_t), (const, override));
    MOCK_METHOD(uint64_t, GetOffset, (docid_t), (const, override));

public:
    ~MockUpdatableVarNumAttributeSegmentReader();
    uint8_t* AllocateBuffer(size_t size);

private:
    std::vector<uint8_t*> mBufferVec;
};

template <typename T>
class MockVarNumAttributePatchReader : public VarNumAttributePatchReader<T>
{
public:
    MOCK_METHOD(void, AddPatchFile, (const std::string& fileName, segmentid_t segmentId), (override));
    MOCK_METHOD(size_t, Seek, (docid_t docId, uint8_t* value, size_t maxLen), (override));

public:
    ~MockVarNumAttributePatchReader();
    uint8_t* AllocateBuffer(size_t size);

private:
    std::vector<uint8_t*> mBufferVec;
};

template <typename T>
class MockVarNumAttributePatchMerger : public VarNumAttributePatchMerger<T>
{
public:
    typedef typename VarNumAttributePatchMerger<T>::PatchFileList PatchFileList;

public:
    MOCK_METHOD(void, Merge, (const PatchFileList& patchFileList, const std::string& destPatchFile), (override));
};

template <typename T>
class VarNumAttributeTestUtil
{
public:
    typedef std::shared_ptr<VarNumAttributePatchReader<T>> PatchReaderPtr;

public:
    /*
     * Format:
     * dataAndPatchString: "offset1:v11,v12,...:p11,p12,...|offset2:NULL:p21|
     *                      offset3:v31:NULL|offset4:v41:;
     *                      ..."
     * "NULL:p21" means null value from data, p21 from patch
     * "v31:NULL" means v31 from data, null value from patch
     * "v41:"     means v41 from data, has no patch
     *
     * e.g.       segment0: docid0->1,2;  docid1->;
     *            patch0  :            ;  docid1->3,4;
     *            segment1:
     *            patch1  :
     *            segment2: docid0->5;
     *            patch2  : docid0->;
     *            string  : "1,2: | NULL:3,4 ; ; 5:NULL"
     */
    static uint32_t CreateAttributeReadersAndPatchReaders(
        const std::string& dataAndPatchString, std::vector<AttributeSegmentReaderPtr>& attributeReaderVec,
        std::vector<typename VarNumAttributeTestUtil<T>::PatchReaderPtr>& patchReaderVec);

    /*
     * Format:
     * expectDataAndOffsetString: offset1:v11,v12,...|offset2:v21,v22,...|...
     * For uniq encode          : offset may use "offset,count"
     *                            if omit [,count] count=count(v11,v12,...)
     * if value is empty, use NULL instead
     * ATTENTION: offsets and values meet the file write sequence,
     *            so values may not match corresponding offset.
     */
    static void CreateExpectMergedDataAndOffset(const std::string& expectDataAndOffsetString,
                                                std::vector<std::string>& dataVec, std::vector<std::string>& offsetVec);

private:
    static void InstallOneDocInAttributeReader(docid_t docId, const std::string& offsetString,
                                               const std::string& dataString,
                                               MockUpdatableVarNumAttributeSegmentReader* attributeReader);
    static void InstallOneDocInPatchReader(docid_t docId, const std::string& patchString,
                                           MockVarNumAttributePatchReader<T>* patchReader);
};

////////////////////////////////////////////////////////
template <typename T>
MockVarNumAttributePatchReader<T>::~MockVarNumAttributePatchReader()
{
    for (size_t i = 0; i < mBufferVec.size(); ++i) {
        delete[] mBufferVec[i];
    }
}

template <typename T>
uint8_t* MockVarNumAttributePatchReader<T>::AllocateBuffer(size_t size)
{
    uint8_t* buffer = new uint8_t[size];
    mBufferVec.push_back(buffer);
    return buffer;
}

template <typename T>
uint32_t VarNumAttributeTestUtil<T>::CreateAttributeReadersAndPatchReaders(
    const std::string& dataAndPatchString, std::vector<AttributeSegmentReaderPtr>& attributeReaderVec,
    std::vector<typename VarNumAttributeTestUtil<T>::PatchReaderPtr>& patchReaderVec)
{
    autil::StringTokenizer stSegs;
    stSegs.tokenize(dataAndPatchString, ";", autil::StringTokenizer::TOKEN_TRIM);
    for (size_t segId = 0; segId < stSegs.getNumTokens(); ++segId) {
        MockUpdatableVarNumAttributeSegmentReader* mockAttributeReader =
            new NiceMock<MockUpdatableVarNumAttributeSegmentReader>;
        AttributeSegmentReaderPtr attributeReader(mockAttributeReader);
        MockVarNumAttributePatchReader<T>* mockPatchReader = new NiceMock<MockVarNumAttributePatchReader<T>>;
        typename VarNumAttributeTestUtil<T>::PatchReaderPtr patchReader(mockPatchReader);

        autil::StringTokenizer stInOneSeg;
        stInOneSeg.tokenize(stSegs[segId], "|",
                            autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
        for (size_t docId = 0; docId < stInOneSeg.getNumTokens(); ++docId) {
            autil::StringTokenizer stDoc;
            stDoc.tokenize(stInOneSeg[docId], ":", autil::StringTokenizer::TOKEN_TRIM);
            assert(3 == stDoc.getNumTokens());
            const std::string& offsetString = stDoc[0];
            const std::string& dataString = stDoc[1];
            const std::string& patchString = stDoc[2];
            InstallOneDocInAttributeReader(docId, offsetString, dataString, mockAttributeReader);
            InstallOneDocInPatchReader(docId, patchString, mockPatchReader);
        }
        attributeReaderVec.push_back(attributeReader);
        patchReaderVec.push_back(patchReader);
    }
    return stSegs.getNumTokens();
}

template <typename T>
void VarNumAttributeTestUtil<T>::InstallOneDocInAttributeReader(
    docid_t docId, const std::string& offsetString, const std::string& dataString,
    MockUpdatableVarNumAttributeSegmentReader* attributeReader)
{
    autil::StringTokenizer stData;
    stData.tokenize(dataString, ",", autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    uint16_t num = (uint16_t)stData.getNumTokens();

    assert(num > 0);

    if (1 == num && stData[0] == std::string("NULL")) {
        num = 0;
    }
    size_t dataLen = sizeof(num) + sizeof(T) * num;
    uint8_t* buffer = attributeReader->AllocateBuffer(dataLen);
    *(uint16_t*)buffer = num;
    T* valueArray = (T*)(buffer + sizeof(num));
    for (size_t i = 0; i < num; ++i) {
        valueArray[i] = autil::StringUtil::fromString<T>(stData[i]);
    }

    if (!offsetString.empty()) {
        uint64_t offset = autil::StringUtil::fromString<uint64_t>(offsetString);
        ON_CALL(*attributeReader, GetOffset(docId)).WillByDefault(Return(offset));
    }
    ON_CALL(*attributeReader, ReadDataAndLen(docId, NotNull(), _, _))
        .WillByDefault(DoAll(SetArrayArgument<1>(buffer, buffer + dataLen), SetArgReferee<3>(dataLen), Return(true)));
}

template <typename T>
void VarNumAttributeTestUtil<T>::InstallOneDocInPatchReader(docid_t docId, const std::string& patchString,
                                                            MockVarNumAttributePatchReader<T>* patchReader)
{
    autil::StringTokenizer stPatch;
    stPatch.tokenize(patchString, ",", autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    uint16_t num = (uint16_t)stPatch.getNumTokens();
    if (0 == num) {
        ON_CALL(*patchReader, Seek(docId, _, _)).WillByDefault(Return(0));
        return;
    } else if (1 == num && stPatch[0] == std::string("NULL")) {
        num = 0;
    }

    size_t bufferSize = sizeof(uint16_t) + sizeof(T) * num;
    uint8_t* buffer = patchReader->AllocateBuffer(bufferSize);
    *(uint16_t*)buffer = num;
    T* valueArray = (T*)(buffer + sizeof(uint16_t));
    for (size_t i = 0; i < num; ++i) {
        valueArray[i] = autil::StringUtil::fromString<T>(stPatch[i]);
    }
    ON_CALL(*patchReader, Seek(docId, NotNull(), _))
        .WillByDefault(DoAll(SetArrayArgument<1>(buffer, buffer + bufferSize), Return(bufferSize)));
}

template <typename T>
void VarNumAttributeTestUtil<T>::CreateExpectMergedDataAndOffset(const std::string& expectDataAndOffsetString,
                                                                 std::vector<std::string>& dataVec,
                                                                 std::vector<std::string>& offsetVec)
{
    autil::StringTokenizer stDataAndOffset;
    stDataAndOffset.tokenize(expectDataAndOffsetString, "|",
                             autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);

    for (size_t docId = 0; docId < stDataAndOffset.getNumTokens(); ++docId) {
        autil::StringTokenizer stDataAndOffsetInOneDoc;
        stDataAndOffsetInOneDoc.tokenize(stDataAndOffset[docId], ":", autil::StringTokenizer::TOKEN_TRIM);

        assert(2 == stDataAndOffsetInOneDoc.getNumTokens());

        const std::string& offsetString = stDataAndOffsetInOneDoc[0];
        const std::string& dataString = stDataAndOffsetInOneDoc[1];

        autil::StringTokenizer stValues;
        stValues.tokenize(dataString, ",",
                          autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
        uint16_t num = (uint16_t)stValues.getNumTokens();
        if (0 == num) {
            ;
        } else if (1 == num && stValues[0] == "NULL") {
            num = 0;
            dataVec.push_back(std::string((char*)&num, sizeof(num)));
        } else {
            T values[num];
            for (size_t i = 0; i < num; ++i) {
                values[i] = autil::StringUtil::fromString<uint32_t>(stValues[i]);
            }
            std::string dataValue((char*)&num, sizeof(num));
            dataValue += std::string((char*)values, sizeof(values));
            dataVec.push_back(dataValue);
        }
        uint64_t offset = autil::StringUtil::fromString<uint64_t>(offsetString);
        offsetVec.push_back(std::string((char*)&offset, sizeof(offset)));
    }
}
}} // namespace indexlib::index
