#ifndef ISEARCH_FAKEMULTIVALUEATTRIBUTEREADER_H
#define ISEARCH_FAKEMULTIVALUEATTRIBUTEREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/FileUtil.h>
#include <indexlib/util/simple_pool.h>
#include <indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h>
#include <indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h>
#include <indexlib/index/normal/attribute/accessor/multi_string_attribute_writer.h>
#include <indexlib/index/normal/attribute/accessor/string_attribute_writer.h>
#include <indexlib/index/normal/attribute/accessor/string_attribute_reader.h>
#include <indexlib/common/field_format/attribute/attribute_convertor_factory.h>
#include <ha3_sdk/testlib/index/IndexDirectoryCreator.h>

IE_NAMESPACE_BEGIN(index);

template <typename T>
class FakeMultiValueAttributeReader : public VarNumAttributeReader<T>
{

public:
    FakeMultiValueAttributeReader() {};
    virtual ~FakeMultiValueAttributeReader() {}

public:
    void setAttributeValues(const std::string &strValues);
private:
    void makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                          const file_system::DirectoryPtr& directory);
private:
    FakeMultiValueAttributeReader(const FakeMultiValueAttributeReader &);
    FakeMultiValueAttributeReader& operator=(const FakeMultiValueAttributeReader &);

private:
    std::vector<std::string> _values;
    std::vector<file_system::DirectoryPtr> _directories;
};

template<typename T>
inline void FakeMultiValueAttributeReader<T>::setAttributeValues(const std::string &strValues)
{
    _values = autil::StringUtil::split(strValues, "#", false);
    for (size_t i = 0; i < _values.size(); ++i) {
        for (size_t j = 0; j < _values[i].size(); ++j) {
            if (_values[i][j] == ',') {
                _values[i][j] = '';
            }
        }
    }

    // make config
    config::AttributeConfigPtr attrConfig(new config::AttributeConfig);
    attrConfig->SetAttrId(0);
    config::FieldConfigPtr fieldConfig(new config::FieldConfig("attr_name",
                    common::TypeInfo<T>::GetFieldType(), true, false));
    attrConfig->Init(fieldConfig);

    std::stringstream ss;
    ss << "../attribute_temp_path_" << pthread_self() << "/";
    std::string path = ss.str();

    if (HA3_NS(util)::FileUtil::localDirExist(path)) {
        HA3_NS(util)::FileUtil::removeLocalDir(path, true);
    }
    HA3_NS(util)::FileUtil::makeLocalDir(path);
    file_system::DirectoryPtr directory = IndexDirectoryCreator::Create(path);

    makeAttrFileData(attrConfig, directory);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = _values.size();
    index_base::SegmentData segmentData;
    segmentData.SetSegmentInfo(segInfo);
    segmentData.SetBaseDocId(0);

    MultiValueAttributeSegmentReader<T> *reader =
        new MultiValueAttributeSegmentReader<T>(attrConfig);
    typename VarNumAttributeReader<T>::SegmentReaderPtr readerPtr(reader);

    reader->Open(segmentData, directory->GetDirectory("attr_name", true));
    this->mSegmentReaders.push_back(readerPtr);
    this->mSegmentInfos.push_back(segInfo);
    this->_directories.push_back(directory);
    HA3_NS(util)::FileUtil::removeLocalDir(path, true);
}

template<typename T>
inline void FakeMultiValueAttributeReader<T>::makeAttrFileData(
        const config::AttributeConfigPtr &attrConfig,
        const file_system::DirectoryPtr& directory) {
    VarNumAttributeWriter<T> writer(attrConfig);
    //FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(common::AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    for (uint32_t i = 0; i < _values.size(); ++i) {
        std::string encodeStr = convertor->Encode(_values[i]);
        writer.AddField((docid_t)i, autil::ConstString(encodeStr));
    }
    util::SimplePool dumpPool;
    writer.Dump(directory, &dumpPool);
}

template<>
inline void FakeMultiValueAttributeReader<autil::MultiChar>::makeAttrFileData(
        const config::AttributeConfigPtr &attrConfig,
        const file_system::DirectoryPtr& directory) {
    MultiStringAttributeWriter writer(attrConfig);
    //FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(common::AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    for (uint32_t i = 0; i < _values.size(); ++i) {
        std::string encodeStr = convertor->Encode(_values[i]);
        writer.AddField((docid_t)i, autil::ConstString(encodeStr));
    }
    util::SimplePool dumpPool;
    writer.Dump(directory, &dumpPool);
}

class FakeStringAttributeReader : public StringAttributeReader
{
public:
    void setAttributeValues(const std::string &strValues);
private:
    void makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                          const file_system::DirectoryPtr& directory);
private:
    std::vector<std::string> _values;
    std::vector<file_system::DirectoryPtr> _directories;
};


inline void FakeStringAttributeReader::setAttributeValues(const std::string &strValues)
{
    // make config
    config::AttributeConfigPtr attrConfig(new config::AttributeConfig);
    attrConfig->SetAttrId(0);
    config::FieldConfigPtr fieldConfig(new config::FieldConfig("attr_name",
                    ft_string, false, false));
    attrConfig->Init(fieldConfig);

    autil::StringUtil::fromString(strValues, _values, ",");
    std::stringstream ss;
    ss << "../attribute_temp_path_" << pthread_self() << "/";
    std::string path = ss.str();

    if (HA3_NS(util)::FileUtil::localDirExist(path)) {
        HA3_NS(util)::FileUtil::removeLocalDir(path, true);
    }
    HA3_NS(util)::FileUtil::makeLocalDir(path);
    file_system::DirectoryPtr directory = IndexDirectoryCreator::Create(path);

    makeAttrFileData(attrConfig, directory);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = _values.size();
    index_base::SegmentData segmentData;
    segmentData.SetSegmentInfo(segInfo);
    segmentData.SetBaseDocId(0);

    VarNumAttributeReader<char>::SegmentReaderPtr
        readerPtr(new MultiValueAttributeSegmentReader<char>(attrConfig));
    readerPtr->Open(segmentData, directory->GetDirectory("attr_name", true));
    mSegmentReaders.push_back(readerPtr);
    mSegmentInfos.push_back(segInfo);
    _directories.push_back(directory);
    HA3_NS(util)::FileUtil::removeLocalDir(path, true);
}

inline void FakeStringAttributeReader::makeAttrFileData(
        const config::AttributeConfigPtr &attrConfig,
        const file_system::DirectoryPtr &directory) {
    StringAttributeWriter writer(attrConfig);
    //FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(common::AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    writer.SetAttrConvertor(convertor);

    writer.Init(FSWriterParamDeciderPtr(), NULL);
    for (uint32_t i = 0; i < _values.size(); ++i) {
        std::string encodeStr = convertor->Encode(_values[i]);
        writer.AddField((docid_t)i, autil::ConstString(encodeStr));
    }
    util::SimplePool dumpPool;
    writer.Dump(directory, &dumpPool);
}

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEMULTIVALUEATTRIBUTEREADER_H
