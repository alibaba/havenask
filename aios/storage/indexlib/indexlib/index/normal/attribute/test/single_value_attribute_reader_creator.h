#pragma once

#include <memory>
#include <time.h>

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeReaderCreator
{
public:
    SingleValueAttributeReaderCreator(const file_system::DirectoryPtr& directory,
                                      const index_base::PartitionMetaPtr& partMeta, int32_t baseSegId = 0)
        : mDirectory(directory)
        , mPartMeta(partMeta)
    {
        srand(time(NULL));
        mRoot = mDirectory->GetOutputPath() + "/";
        mBaseSegId = baseSegId;
    }
    ~SingleValueAttributeReaderCreator() {}

public:
    std::shared_ptr<SingleValueAttributeReader<T>> Create(const std::vector<uint32_t>& docCounts,
                                                          const std::vector<std::vector<T>>& data,
                                                          const std::string& attrName);
    std::shared_ptr<SingleValueAttributeReader<T>> Create(const std::vector<uint32_t>& docCounts,
                                                          const std::vector<std::vector<std::pair<T, bool>>>& data,
                                                          const std::string& attrName);

private:
    void FullBuild(const std::vector<uint32_t>& docCounts, const std::vector<std::vector<std::pair<T, bool>>>& datas);
    void CreateDataForOneSegment(segmentid_t segId, uint32_t docCount, const std::vector<std::pair<T, bool>>& data);
    void WriteSegmentInfo(segmentid_t segId, const index_base::SegmentInfo& segInfo);

private:
    std::string mRoot;
    file_system::DirectoryPtr mDirectory;
    config::AttributeConfigPtr mAttrConfig;
    index_base::PartitionMetaPtr mPartMeta;
    segmentid_t mBaseSegId = {0};

private:
    IE_LOG_DECLARE();
};

template <typename T>
inline std::shared_ptr<SingleValueAttributeReader<T>>
SingleValueAttributeReaderCreator<T>::Create(const std::vector<uint32_t>& docCounts,
                                             const std::vector<std::vector<T>>& expectedData,
                                             const std::string& attrName)
{
    mAttrConfig = AttributeTestUtil::CreateAttrConfig(common::TypeInfo<T>::GetFieldType(), false, attrName, 0, false);
    std::vector<std::vector<std::pair<T, bool>>> data;
    data.resize(expectedData.size());
    for (size_t i = 0; i < expectedData.size(); i++) {
        for (size_t j = 0; j < expectedData[i].size(); j++) {
            data[i].push_back(std::make_pair(expectedData[i][j], false));
        }
    }
    FullBuild(docCounts, data);
    uint32_t segCount = docCounts.size();
    merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(mDirectory, segCount);
    segDir->Init(false);
    std::shared_ptr<SingleValueAttributeReader<T>> reader;
    reader.reset(new SingleValueAttributeReader<T>);
    reader->Open(mAttrConfig, segDir->GetPartitionData(), PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    return reader;
}

template <typename T>
inline std::shared_ptr<SingleValueAttributeReader<T>>
SingleValueAttributeReaderCreator<T>::Create(const std::vector<uint32_t>& docCounts,
                                             const std::vector<std::vector<std::pair<T, bool>>>& expectedData,
                                             const std::string& attrName)
{
    mAttrConfig =
        AttributeTestUtil::CreateAttrConfig(common::TypeInfo<T>::GetFieldType(), false, attrName, 0, false, true);
    std::vector<std::vector<std::pair<T, bool>>> data;
    FullBuild(docCounts, expectedData);
    uint32_t segCount = docCounts.size();
    merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(mDirectory, segCount, mBaseSegId);
    segDir->Init(false);
    std::shared_ptr<SingleValueAttributeReader<T>> reader;
    reader.reset(new SingleValueAttributeReader<T>);
    reader->Open(mAttrConfig, segDir->GetPartitionData(), PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    mBaseSegId += docCounts.size();
    return reader;
}

template <typename T>
inline void SingleValueAttributeReaderCreator<T>::FullBuild(const std::vector<uint32_t>& docCounts,
                                                            const std::vector<std::vector<std::pair<T, bool>>>& datas)
{
    IndexTestUtil::ResetDir(mRoot);
    mPartMeta->Store(mDirectory);
    for (uint32_t i = 0; i < docCounts.size(); i++) {
        segmentid_t segId = i + mBaseSegId;
        CreateDataForOneSegment(segId, docCounts[i], datas[i]);
        index_base::SegmentInfo segInfo;
        segInfo.docCount = docCounts[i];
        WriteSegmentInfo(segId, segInfo);
    }
}

template <typename T>
inline void
SingleValueAttributeReaderCreator<T>::CreateDataForOneSegment(segmentid_t segId, uint32_t docCount,
                                                              const std::vector<std::pair<T, bool>>& expectedData)
{
    SingleValueAttributeWriter<T> writer(mAttrConfig);
    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(mAttrConfig));
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    assert(convertor);
    autil::mem_pool::Pool pool;

    for (uint32_t i = 0; i < docCount; ++i) {
        T value = expectedData[i].first;
        bool isNull = expectedData[i].second;
        std::string strValue = autil::StringUtil::toString(value);
        docid_t localDocId = i;
        autil::StringView encodeValue = convertor->Encode(autil::StringView(strValue), &pool);
        writer.AddField(localDocId, encodeValue, isNull);
    }

    std::stringstream path;
    path << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/" << ATTRIBUTE_DIR_NAME;
    file_system::DirectoryPtr attrDirectory = mDirectory->MakeDirectory(path.str());
    util::SimplePool dumpPool;
    writer.Dump(attrDirectory, &dumpPool);
}

template <typename T>
inline void SingleValueAttributeReaderCreator<T>::WriteSegmentInfo(segmentid_t segId,
                                                                   const index_base::SegmentInfo& segInfo)
{
    std::string segName = std::string(SEGMENT_FILE_NAME_PREFIX) + "_" + std::to_string(segId) + "_level_0/";
    auto segDir = mDirectory->MakeDirectory(segName);
    assert(segDir != nullptr);
    segInfo.Store(segDir);
}

// DEFINE_SHARED_PTR(SingleValueAttributeReaderCreator);
}} // namespace indexlib::index
