#ifndef ISEARCH_FAKEMULTIVALUEATTRIBUTEREADER_H
#define ISEARCH_FAKEMULTIVALUEATTRIBUTEREADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include <indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h>
#include <indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h>
#include <indexlib/index/normal/attribute/accessor/multi_string_attribute_writer.h>
#include <indexlib/index/normal/attribute/accessor/string_attribute_writer.h>
#include <indexlib/index/normal/attribute/accessor/string_attribute_reader.h>
#include <indexlib/file_system/indexlib_file_system_impl.h>
#include <indexlib/file_system/directory_creator.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/config/load_config_list.h>

IE_NAMESPACE_BEGIN(testlib);

template <typename T>
class FakeMultiValueAttributeReader : public index::VarNumAttributeReader<T>
{

public:
    FakeMultiValueAttributeReader(const std::string &rootPath) {
        std::stringstream ss;
        if (!rootPath.empty()) {
            ss << rootPath << "/attribute_temp_path_" << pthread_self() << "/";
        } else {
            ss << "./attribute_temp_path_" << pthread_self() << "/";
        }
        mPath = ss.str();
        storage::FileSystemWrapper::DeleteIfExist(mPath);
        storage::FileSystemWrapper::MkDir(mPath);

        file_system::FileSystemOptions options;
        config::LoadConfigList loadConfigList;
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = false;
        options.needFlush = true;
        options.useCache = true;
        util::MemoryQuotaControllerPtr mqc(
                new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        util::PartitionMemoryQuotaControllerPtr quotaController(
                new util::PartitionMemoryQuotaController(mqc));
        options.memoryQuotaController = quotaController;
        this->mFileSystem.reset(new file_system::IndexlibFileSystemImpl(mPath));
        this->mFileSystem->Init(options);
    }
    virtual ~FakeMultiValueAttributeReader() {
        storage::FileSystemWrapper::DeleteIfExist(mPath);
    }

public:
    void setAttributeValues(const std::string &strValues);
private:
    void makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                          const file_system::DirectoryPtr& directory);
private:
    FakeMultiValueAttributeReader(const FakeMultiValueAttributeReader &);
    FakeMultiValueAttributeReader& operator=(const FakeMultiValueAttributeReader &);

private:
    std::string mPath;
    file_system::IndexlibFileSystemPtr mFileSystem;
    std::vector<std::string> mValues;
    std::vector<file_system::DirectoryPtr> mDirectories;
};

template<typename T>
inline void FakeMultiValueAttributeReader<T>::setAttributeValues(const std::string &strValues)
{
    autil::StringUtil::fromString(strValues, mValues, "#");
    for (size_t i = 0; i < mValues.size(); ++i) {
        for (size_t j = 0; j < mValues[i].size(); ++j) {
            if (mValues[i][j] == ',') {
                mValues[i][j] = '';
            }
        }
    }

    // make config
    config::AttributeConfigPtr attrConfig(new config::AttributeConfig);
    attrConfig->SetAttrId(0);
    config::FieldConfigPtr fieldConfig(new config::FieldConfig("attr_name",
                    common::TypeInfo<T>::GetFieldType(), true));
    attrConfig->Init(fieldConfig);

    file_system::DirectoryPtr directory =
        file_system::DirectoryCreator::Get(this->mFileSystem, mPath, true);

    makeAttrFileData(attrConfig, directory);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = mValues.size();
    index_base::SegmentData segmentData;
    segmentData.SetSegmentInfo(segInfo);
    segmentData.SetBaseDocId(0);
    
    index::MultiValueAttributeSegmentReader<T> *reader = 
        new index::MultiValueAttributeSegmentReader<T>(attrConfig);
    typename index::VarNumAttributeReader<T>::SegmentReaderPtr readerPtr(reader);

    reader->Open(segmentData, directory->GetDirectory("attr_name", true));
    this->mSegmentReaders.push_back(readerPtr);
    this->mSegmentInfos.push_back(segInfo);
    this->mDirectories.push_back(directory);
}

template<typename T>
inline void FakeMultiValueAttributeReader<T>::makeAttrFileData(
        const config::AttributeConfigPtr &attrConfig,
        const file_system::DirectoryPtr& directory) {
    autil::mem_pool::Pool pool;
    index::VarNumAttributeWriter<T> writer(attrConfig);
    //FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(common::AttributeConvertorFactory::GetInstance()
                                    ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    writer.SetAttrConvertor(convertor);
    writer.Init(index::FSWriterParamDeciderPtr(), NULL);
    for (uint32_t i = 0; i < mValues.size(); ++i) {
        std::string encodeStr = convertor->Encode(mValues[i]);
        writer.AddField((docid_t)i, autil::ConstString(encodeStr));
    }
    writer.Dump(directory, &pool);
}

template<>
inline void FakeMultiValueAttributeReader<autil::MultiChar>::makeAttrFileData(
        const config::AttributeConfigPtr &attrConfig,
        const file_system::DirectoryPtr& directory) {
    autil::mem_pool::Pool pool;
    index::MultiStringAttributeWriter writer(attrConfig);
    //FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(common::AttributeConvertorFactory::GetInstance()
                                    ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    writer.SetAttrConvertor(convertor);
    writer.Init(index::FSWriterParamDeciderPtr(), NULL);
    for (uint32_t i = 0; i < mValues.size(); ++i) {
        std::string encodeStr = convertor->Encode(mValues[i]);
        writer.AddField((docid_t)i, autil::ConstString(encodeStr));
    }
    writer.Dump(directory, &pool);
}

class FakeStringAttributeReader : public index::StringAttributeReader
{
public:
    FakeStringAttributeReader(const std::string &rootPath) {
        std::stringstream ss;
        if (!rootPath.empty()) {
            ss << rootPath << "/attribute_temp_path_" << pthread_self() << "/";
        } else {
            ss << "./attribute_temp_path_" << pthread_self() << "/";
        }
        mPath = ss.str();
        storage::FileSystemWrapper::DeleteIfExist(mPath);
        storage::FileSystemWrapper::MkDir(mPath);

        file_system::FileSystemOptions options;
        config::LoadConfigList loadConfigList;
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = false;
        options.needFlush = true;
        options.useCache = true;
        this->mFileSystem.reset(new file_system::IndexlibFileSystemImpl(mPath));
        util::MemoryQuotaControllerPtr mqc(
                new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        util::PartitionMemoryQuotaControllerPtr quotaController(
                new util::PartitionMemoryQuotaController(mqc));
        options.memoryQuotaController = quotaController;
        this->mFileSystem->Init(options);
    }
    ~FakeStringAttributeReader() {
        storage::FileSystemWrapper::DeleteIfExist(mPath);
    }
public:
    void setAttributeValues(const std::string &strValues, const std::string &stringValueSep);
private:
    void makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                          const file_system::DirectoryPtr& directory);
private:
    std::string mPath;
    file_system::IndexlibFileSystemPtr mFileSystem;
    std::vector<std::string> mValues;
    std::vector<file_system::DirectoryPtr> mDirectories;
};

inline void FakeStringAttributeReader::setAttributeValues(
        const std::string &strValues, const std::string &stringValueSep)
{
    // make config
    config::AttributeConfigPtr attrConfig(new config::AttributeConfig);
    attrConfig->SetAttrId(0);
    config::FieldConfigPtr fieldConfig(new config::FieldConfig("attr_name",
                    ft_string, false));
    attrConfig->Init(fieldConfig);

    autil::StringUtil::fromString(strValues, mValues, stringValueSep);

    file_system::DirectoryPtr directory =
        file_system::DirectoryCreator::Get(this->mFileSystem, mPath, true);

    makeAttrFileData(attrConfig, directory);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = mValues.size();
    index_base::SegmentData segmentData;
    segmentData.SetSegmentInfo(segInfo);
    segmentData.SetBaseDocId(0);

    index::VarNumAttributeReader<char>::SegmentReaderPtr 
        readerPtr(new index::MultiValueAttributeSegmentReader<char>(attrConfig));
    readerPtr->Open(segmentData, directory->GetDirectory("attr_name", true));
    mSegmentReaders.push_back(readerPtr);
    mSegmentInfos.push_back(segInfo);
    mDirectories.push_back(directory);
}

inline void FakeStringAttributeReader::makeAttrFileData(
        const config::AttributeConfigPtr &attrConfig,
        const file_system::DirectoryPtr &directory) {
    autil::mem_pool::Pool pool;
    index::StringAttributeWriter writer(attrConfig);
    //FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(common::AttributeConvertorFactory::GetInstance()
                                    ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    writer.SetAttrConvertor(convertor);

    writer.Init(index::FSWriterParamDeciderPtr(), NULL);
    for (uint32_t i = 0; i < mValues.size(); ++i) {
        std::string encodeStr = convertor->Encode(mValues[i]);
        writer.AddField((docid_t)i, autil::ConstString(encodeStr));
    }
    writer.Dump(directory, &pool);
}

IE_NAMESPACE_END(testlib);

#endif //ISEARCH_FAKEMULTIVALUEATTRIBUTEREADER_H
