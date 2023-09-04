#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/indexlib.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib {
namespace index {

class FakeJoinDocidAttributeReader : public JoinDocidAttributeReader {
    typedef SingleValueAttributeSegmentReader<docid_t> JoinDocidSegmentReader;
    class FakeSegmentReader : public JoinDocidSegmentReader {
    private:
        struct Config {
            config::FieldConfigPtr DEFAULT_FIELD_CONFIG;
            config::AttributeConfigPtr DEFAULT_ATTR;
            Config() {
                DEFAULT_FIELD_CONFIG.reset(new config::FieldConfig(
                    "", common::TypeInfo<docid_t>::GetFieldType(), false, false));
                DEFAULT_ATTR.reset(new config::AttributeConfig());
                DEFAULT_ATTR->Init(DEFAULT_FIELD_CONFIG);
            }
        };

    public:
        FakeSegmentReader()
            : JoinDocidSegmentReader(Config().DEFAULT_ATTR) {}

        ~FakeSegmentReader() {
            delete[](docid_t *) this->mData;
            this->mData = NULL;
        }

    public:
        void open(docid_t *data, uint32_t docCount) {
            this->mData = (uint8_t *)(new docid_t[docCount]);
            memcpy(this->mData, data, docCount * sizeof(docid_t));
            this->mDocCount = docCount;
        }
    };

public:
    FakeJoinDocidAttributeReader() {}
    virtual ~FakeJoinDocidAttributeReader() {}

public:
    /* override */ bool
    Read(docid_t docId, std::string &attrValue, autil::mem_pool::Pool *pool = NULL) const override {
        docid_t value
            = (size_t)docId < _values.size() ? _values[docId] : std::numeric_limits<docid_t>::max();
        attrValue = autil::StringUtil::toString(value);
        return true;
    }

public:
    void setAttributeValues(const std::string &strValues);
    void setAttributeValues(const std::vector<docid_t> &values);
    void SetSortType(indexlibv2::config::SortPattern sortType) {
        this->mSortType = sortType;
    }

private:
    std::vector<docid_t> _values;

private:
    AUTIL_LOG_DECLARE();
};

static inline void convertStringToValues(std::vector<docid_t> &values,
                                         const std::string &strValues) {
    autil::StringUtil::fromString(strValues, values, ",");
}

static inline void convertStringToValues(std::vector<std::vector<docid_t>> &values,
                                         const std::string &strValues) {
    autil::StringUtil::fromString(strValues, values, ",", "#");
}

inline void FakeJoinDocidAttributeReader::setAttributeValues(const std::string &strValues) {
    convertStringToValues(_values, strValues);
    setAttributeValues(_values);
}

inline void FakeJoinDocidAttributeReader::setAttributeValues(const std::vector<docid_t> &values) {
    _values = values;

    FakeSegmentReader *reader = new FakeSegmentReader();
    reader->open((docid_t *)&(_values[0]), (uint32_t)_values.size());

    SegmentReaderPtr readerPtr(reader);
    this->mSegmentReaders.push_back(readerPtr);

    uint64_t docCount = (uint32_t)_values.size();
    this->mSegmentDocCount.push_back(docCount);

    // TODO: baseDocId set to 0, no set building segment
    this->mJoinedBaseDocIds.push_back(0);
}

} // namespace index
} // namespace indexlib
