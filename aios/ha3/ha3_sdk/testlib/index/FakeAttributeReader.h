#pragma once

#include <assert.h>
#include <limits>
#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib {
namespace index {

template <typename T>
class FakeAttributeReader : public indexlib::index::SingleValueAttributeReader<T> {
private:
    struct Config {
        config::FieldConfigPtr DEFAULT_FIELD_CONFIG;
        config::AttributeConfigPtr DEFAULT_ATTR;
        Config() {
            DEFAULT_FIELD_CONFIG.reset(
                new config::FieldConfig("", common::TypeInfo<T>::GetFieldType(), false, false));
            DEFAULT_ATTR.reset(new config::AttributeConfig());
            DEFAULT_ATTR->Init(DEFAULT_FIELD_CONFIG);
        }
    };

public:
    class FakeSegmentReader : public indexlib::index::SingleValueAttributeSegmentReader<T> {
    public:
        FakeSegmentReader()
            : indexlib::index::SingleValueAttributeSegmentReader<T>(Config().DEFAULT_ATTR) {
            this->mPatchApplyStrategy = PatchApplyStrategy::PAS_APPLY_NO_PATCH;
        }
        FakeSegmentReader(uint32_t docCount)
            : indexlib::index::SingleValueAttributeSegmentReader<T>(Config().DEFAULT_ATTR) {
            this->mPatchApplyStrategy = PatchApplyStrategy::PAS_APPLY_NO_PATCH;
            this->mDocCount = docCount;
            this->mData = new uint8_t[this->mDocCount * sizeof(T)];
            this->mFormatter.Init(0, sizeof(T));
        }
        ~FakeSegmentReader() {
            release();
        }

    public:
        void open(T *data, uint32_t docCount) {
            release();
            this->mData = (uint8_t *)(new T[docCount]);
            memcpy(this->mData, data, docCount * sizeof(T));
            this->mDocCount = docCount;
        }

    private:
        void release() {
            if (this->mData) {
                delete[](T *) this->mData;
                this->mData = NULL;
            }
        }
    };

public:
    typedef T ValueType;

public:
    FakeAttributeReader() {
        SingleValueAttributeReader<T>::mAttrConfig = Config().DEFAULT_ATTR;
    }
    virtual ~FakeAttributeReader() {}

public:
    bool
    Read(docid_t docId, std::string &attrValue, autil::mem_pool::Pool *pool = NULL) const override {
        T value = (size_t)docId < _values.size() ? _values[docId] : std::numeric_limits<T>::max();
        attrValue = autil::StringUtil::toString(value);
        return true;
    }

public:
    void setAttributeValues(const std::string &strValues);
    void setAttributeValues(const std::vector<ValueType> &values);
    void SetSortType(indexlibv2::config::SortPattern sortType) {
        SingleValueAttributeReader<T>::mSortType = sortType;
    }

private:
    std::vector<T> _values;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
static inline void convertStringToValues(std::vector<T> &values, const std::string &strValues) {
    autil::StringUtil::fromString(strValues, values, ",");
}

template <typename T>
static inline void convertStringToValues(std::vector<std::vector<T>> &values,
                                         const std::string &strValues) {
    autil::StringUtil::fromString(strValues, values, ",", "#");
}

template <typename T>
void FakeAttributeReader<T>::setAttributeValues(const std::string &strValues) {
    convertStringToValues(_values, strValues);
    setAttributeValues(_values);
}

template <typename T>
void FakeAttributeReader<T>::setAttributeValues(const std::vector<ValueType> &values) {
    _values = values;

    FakeSegmentReader *reader = new FakeSegmentReader();
    typename SingleValueAttributeReader<T>::SegmentReaderPtr readerPtr(reader);
    reader->open((T *)&(_values[0]), (uint32_t)_values.size());
    this->mSegmentReaders.push_back(readerPtr);
    index_base::SegmentInfo segInfo;
    uint64_t docCount = (uint64_t)_values.size();
    this->mSegmentDocCount.push_back(docCount);
}

} // namespace index
} // namespace indexlib
