#ifndef ISEARCH_FAKE_ATTRIBUTEREADER_H
#define ISEARCH_FAKE_ATTRIBUTEREADER_H

#include <ha3/index/index.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <indexlib/index/normal/attribute/accessor/attribute_reader.h>
#include <indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h>
#include <indexlib/common/field_format/attribute/type_info.h>
#include <autil/StringUtil.h>
#include <sstream>
#include <suez/turing/expression/framework/VariableTypeTraits.h>

IE_NAMESPACE_BEGIN(index);

template <typename T>
class FakeAttributeReader : public index::SingleValueAttributeReader<T>
{
public:
    class FakeSegmentReader : public index::SingleValueAttributeSegmentReader<T>
    {
    private:
        struct Config {
            config::FieldConfigPtr DEFAULT_FIELD_CONFIG;
            config::AttributeConfigPtr DEFAULT_ATTR;
            Config()
            {
                DEFAULT_FIELD_CONFIG.reset(new config::FieldConfig("",
                                common::TypeInfo<T>::GetFieldType(), false, false));
                DEFAULT_ATTR.reset(new config::AttributeConfig());
                DEFAULT_ATTR->Init(DEFAULT_FIELD_CONFIG);
            }
        };
    public:
        FakeSegmentReader()
            : index::SingleValueAttributeSegmentReader<T>(Config().DEFAULT_ATTR)
        {
        }
        FakeSegmentReader(uint32_t docCount)
            : index::SingleValueAttributeSegmentReader<T>(Config().DEFAULT_ATTR)
        {
            this->mDocCount = docCount;
            this->mData = new uint8_t[this->mDocCount * sizeof(T)];
            this->mFormatter.Init(0, sizeof(T));
        }
        ~FakeSegmentReader()
        {
            release();
        }
    public:
        void open(T* data, uint32_t docCount) {
            release();
            this->mData = (uint8_t*)(new T[docCount]);
            memcpy(this->mData, data, docCount * sizeof(T));
            this->mDocCount = docCount;
        }
    private:
        void release() {
            if (this->mData) {
                delete[] (T*)this->mData;
                this->mData = NULL;
            }
        }

    };
public:
    typedef T ValueType;

public:
    FakeAttributeReader() {}
    virtual ~FakeAttributeReader() {}

public:
    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override
    {
        T value = (size_t)docId < _values.size() ? _values[docId] : std::numeric_limits<T>::max();
        attrValue = autil::StringUtil::toString(value);
        return true;
    }

    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const override {
        assert(false);
        return true;
    }
public:
    void setAttributeValues(const std::string &strValues);
    void setAttributeValues(const std::vector<ValueType> &values);
    void SetSortType(SortPattern sortType)
    { SingleValueAttributeReader<T>::mSortType = sortType; }

private:
    std::vector<T> _values;

private:
    HA3_LOG_DECLARE();
};

template<typename T>
static inline void convertStringToValues(std::vector<T> &values, const std::string &strValues)
{
    autil::StringUtil::fromString(strValues, values, ",");
}

template<typename T>
static inline void convertStringToValues(std::vector<std::vector<T> > &values, const std::string &strValues) {
    autil::StringUtil::fromString(strValues, values, ",", "#");
}

template<typename T>
void FakeAttributeReader<T>::setAttributeValues(const std::string &strValues) {
    convertStringToValues(_values, strValues);
    setAttributeValues(_values);
}

template <typename T>
void FakeAttributeReader<T>::setAttributeValues(const std::vector<ValueType> &values) {
    _values = values;

    FakeSegmentReader* reader = new FakeSegmentReader();
    typename SingleValueAttributeReader<T>::SegmentReaderPtr readerPtr(reader);
    reader->open((T*)&(_values[0]), (uint32_t)_values.size());
    this->mSegmentReaders.push_back(readerPtr);
    index_base::SegmentInfo segInfo;
    segInfo.docCount = (uint32_t)_values.size();
    this->mSegmentInfos.push_back(segInfo);
}

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKE_ATTRIBUTEREADER_H
