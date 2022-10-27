#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_CREATOR_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_CREATOR_H

#include <tr1/memory>
#include <time.h>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/File.h"
#include "autil/mem_pool/Pool.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class SingleValueAttributeReaderCreator
{
public:
    SingleValueAttributeReaderCreator(
            const file_system::DirectoryPtr& directory,
            const index_base::PartitionMetaPtr& partMeta)
        : mDirectory(directory)
        , mPartMeta(partMeta)
    {
        srand(time(NULL));
        mRoot = mDirectory->GetPath() + "/";
    }
    ~SingleValueAttributeReaderCreator() {}
public:
    std::tr1::shared_ptr< SingleValueAttributeReader<T> > Create(
            const std::vector<uint32_t>& docCounts,
            const std::vector< std::vector<T> >& data,
            const std::string& attrName);
private:
    void FullBuild(const std::vector<uint32_t>& docCounts, 
                   const std::vector< std::vector<T> >& datas);
    void CreateDataForOneSegment(segmentid_t segId, 
                                 uint32_t docCount, 
                                 const std::vector<T>& data);
    void WriteSegmentInfo(segmentid_t segId,
                          const index_base::SegmentInfo& segInfo);
private:
    std::string mRoot;
    file_system::DirectoryPtr mDirectory;
    config::AttributeConfigPtr mAttrConfig;
    index_base::PartitionMetaPtr mPartMeta;
private:
    IE_LOG_DECLARE();
};

template<typename T>
inline std::tr1::shared_ptr<SingleValueAttributeReader<T> >
SingleValueAttributeReaderCreator<T>::Create(
        const std::vector<uint32_t>& docCounts,
        const std::vector< std::vector<T> >& expectedData,
        const std::string& attrName)
{
    mAttrConfig = AttributeTestUtil::CreateAttrConfig(
            common::TypeInfo<T>::GetFieldType(), false, attrName, 0, false);
    FullBuild(docCounts, expectedData);
    uint32_t segCount = docCounts.size();
    merger::SegmentDirectoryPtr segDir = 
        IndexTestUtil::CreateSegmentDirectory(mRoot, segCount);
    segDir->Init(false);
    std::tr1::shared_ptr<SingleValueAttributeReader<T> > reader;
    reader.reset(new SingleValueAttributeReader<T>);
    reader->Open(mAttrConfig, segDir->GetPartitionData());
    return reader;
}

template<typename T>
inline void SingleValueAttributeReaderCreator<T>::FullBuild(
        const std::vector<uint32_t>& docCounts, 
        const std::vector< std::vector<T> >& datas)
{
    IndexTestUtil::ResetDir(mRoot);
    mPartMeta->Store(mRoot);
    for (uint32_t i = 0; i < docCounts.size(); i++)
    {
        segmentid_t segId = i;
        CreateDataForOneSegment(segId, docCounts[i], 
                                datas[i]);
        index_base::SegmentInfo segInfo;
        segInfo.docCount = docCounts[i];
        WriteSegmentInfo(segId, segInfo);
    }
}

template<typename T>
inline void SingleValueAttributeReaderCreator<T>::CreateDataForOneSegment(
        segmentid_t segId, 
        uint32_t docCount, 
        const std::vector<T>& expectedData)
{
    SingleValueAttributeWriter<T> writer(mAttrConfig);
    common::AttributeConvertorPtr convertor(
            common::AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(mAttrConfig->GetFieldConfig()));
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    assert(convertor);
    writer.SetAttrConvertor(convertor);
    autil::mem_pool::Pool pool;

    for (uint32_t i = 0; i < docCount; ++i)
    {
        T value = expectedData[i];
        std::string strValue = autil::StringUtil::toString(value);
        docid_t localDocId = i;
        autil::ConstString encodeValue = convertor->Encode(autil::ConstString(strValue), &pool);
        writer.AddField(localDocId, encodeValue);
    }

    std::stringstream path;
    path << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/" << ATTRIBUTE_DIR_NAME;
    file_system::DirectoryPtr attrDirectory = 
        mDirectory->MakeDirectory(path.str());
    util::SimplePool dumpPool;
    writer.Dump(attrDirectory, &dumpPool);
}

template <typename T>
inline void SingleValueAttributeReaderCreator<T>::WriteSegmentInfo(
        segmentid_t segId, const index_base::SegmentInfo& segInfo)
{
    std::stringstream segPath;
    segPath << mRoot << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/";
    std::string segInfoPath = segPath.str() + SEGMENT_INFO_FILE_NAME;
    segInfo.Store(segInfoPath);
}

//DEFINE_SHARED_PTR(SingleValueAttributeReaderCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_CREATOR_H
