#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <pthread.h>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/ConstString.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "fslib/util/FileUtil.h"
#include "ha3_sdk/testlib/index/IndexDirectoryCreator.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_string_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib {
namespace index {

template <typename T>
class FakeMultiValueAttributeReader : public VarNumAttributeReader<T> {

public:
    FakeMultiValueAttributeReader() {};
    virtual ~FakeMultiValueAttributeReader() {}

public:
    void setAttributeValues(const std::string &strValues);

private:
    void makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                          const file_system::DirectoryPtr &directory);

private:
    FakeMultiValueAttributeReader(const FakeMultiValueAttributeReader &);
    FakeMultiValueAttributeReader &operator=(const FakeMultiValueAttributeReader &);

private:
    std::vector<std::string> _values;
    std::vector<file_system::DirectoryPtr> _directories;
};

template <typename T>
inline void FakeMultiValueAttributeReader<T>::setAttributeValues(const std::string &strValues) {
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
    config::FieldConfigPtr fieldConfig(
        new config::FieldConfig("attr_name", common::TypeInfo<T>::GetFieldType(), true, false));
    attrConfig->Init(fieldConfig);

    std::stringstream ss;
    ss << "../attribute_temp_path_" << pthread_self() << "/";
    std::string path = ss.str();

    if (fslib::util::FileUtil::isExist(path)) {
        fslib::util::FileUtil::remove(path);
    }
    fslib::util::FileUtil::mkDir(path);
    file_system::DirectoryPtr directory = IndexDirectoryCreator::Create(path);

    makeAttrFileData(attrConfig, directory);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = _values.size();
    index_base::SegmentData segmentData;
    segmentData.SetSegmentInfo(segInfo);
    segmentData.SetBaseDocId(0);

    MultiValueAttributeSegmentReader<T> *reader
        = new MultiValueAttributeSegmentReader<T>(attrConfig);
    typename VarNumAttributeReader<T>::SegmentReaderPtr readerPtr(reader);

    reader->Open(segmentData,
                 PatchApplyOption::NoPatch(),
                 directory->GetDirectory("attr_name", true),
                 nullptr,
                 false);
    this->mSegmentReaders.push_back(readerPtr);
    this->mSegmentDocCount.push_back((uint64_t)segInfo.docCount);
    this->_directories.push_back(directory);
    fslib::util::FileUtil::remove(path);
}

template <typename T>
inline void
FakeMultiValueAttributeReader<T>::makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                                                   const file_system::DirectoryPtr &directory) {
    VarNumAttributeWriter<T> writer(attrConfig);
    // FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    writer.SetAttrConvertor(convertor);
    writer.Init(nullptr, NULL);
    for (uint32_t i = 0; i < _values.size(); ++i) {
        std::string encodeStr = convertor->Encode(_values[i]);
        writer.AddField((docid_t)i, autil::StringView(encodeStr));
    }
    util::SimplePool dumpPool;
    writer.Dump(directory, &dumpPool);
}

template <>
inline void FakeMultiValueAttributeReader<autil::MultiChar>::makeAttrFileData(
    const config::AttributeConfigPtr &attrConfig, const file_system::DirectoryPtr &directory) {
    MultiStringAttributeWriter writer(attrConfig);
    // FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    writer.SetAttrConvertor(convertor);
    writer.Init(nullptr, NULL);
    for (uint32_t i = 0; i < _values.size(); ++i) {
        std::string encodeStr = convertor->Encode(_values[i]);
        writer.AddField((docid_t)i, autil::StringView(encodeStr));
    }
    util::SimplePool dumpPool;
    writer.Dump(directory, &dumpPool);
}

class FakeStringAttributeReader : public StringAttributeReader {
public:
    void setAttributeValues(const std::string &strValues);

private:
    void makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                          const file_system::DirectoryPtr &directory);

private:
    std::vector<std::string> _values;
    std::vector<file_system::DirectoryPtr> _directories;
};

inline void FakeStringAttributeReader::setAttributeValues(const std::string &strValues) {
    // make config
    config::AttributeConfigPtr attrConfig(new config::AttributeConfig);
    attrConfig->SetAttrId(0);
    config::FieldConfigPtr fieldConfig(
        new config::FieldConfig("attr_name", ft_string, false, false));
    attrConfig->Init(fieldConfig);

    autil::StringUtil::fromString(strValues, _values, ",");
    std::stringstream ss;
    ss << "../attribute_temp_path_" << pthread_self() << "/";
    std::string path = ss.str();

    if (fslib::util::FileUtil::isExist(path)) {
        fslib::util::FileUtil::remove(path);
    }
    fslib::util::FileUtil::mkDir(path);
    file_system::DirectoryPtr directory = IndexDirectoryCreator::Create(path);

    makeAttrFileData(attrConfig, directory);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = _values.size();
    index_base::SegmentData segmentData;
    segmentData.SetSegmentInfo(segInfo);
    segmentData.SetBaseDocId(0);

    VarNumAttributeReader<char>::SegmentReaderPtr readerPtr(
        new MultiValueAttributeSegmentReader<char>(attrConfig));
    readerPtr->Open(segmentData,
                    PatchApplyOption::NoPatch(),
                    directory->GetDirectory("attr_name", true),
                    nullptr,
                    false);
    mSegmentReaders.push_back(readerPtr);
    mSegmentDocCount.push_back((uint64_t)segInfo.docCount);
    _directories.push_back(directory);
    fslib::util::FileUtil::remove(path);
}

inline void
FakeStringAttributeReader::makeAttrFileData(const config::AttributeConfigPtr &attrConfig,
                                            const file_system::DirectoryPtr &directory) {
    StringAttributeWriter writer(attrConfig);
    // FakeAttributeConvertorPtr convertor(new FakeAttributeConvertor);
    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    writer.SetAttrConvertor(convertor);

    writer.Init(nullptr, NULL);
    for (uint32_t i = 0; i < _values.size(); ++i) {
        std::string encodeStr = convertor->Encode(_values[i]);
        writer.AddField((docid_t)i, autil::StringView(encodeStr));
    }
    util::SimplePool dumpPool;
    writer.Dump(directory, &dumpPool);
}

} // namespace index
} // namespace indexlib
