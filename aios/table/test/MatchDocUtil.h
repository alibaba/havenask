#pragma once
#include <string>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDocAllocator.h"
#include "unittest/unittest.h"

namespace table {

template <typename T>
struct ColumnDataTypeTraits {
    typedef T value_type;
};
#define REGISTER_COLUMN_DATATYPE_TRAITS(CppType, ColumnDataType)                                                       \
    template <>                                                                                                        \
    struct ColumnDataTypeTraits<CppType> {                                                                             \
        typedef ColumnDataType value_type;                                                                             \
    }
REGISTER_COLUMN_DATATYPE_TRAITS(std::string, autil::MultiChar);

class MatchDocUtil {
public:
    MatchDocUtil(std::shared_ptr<autil::mem_pool::Pool> &dataPool) : _dataPool(dataPool) {}
    MatchDocUtil() : _dataPool(new autil::mem_pool::Pool) {}

public:
    std::vector<matchdoc::MatchDoc> createMatchDocs(matchdoc::MatchDocAllocatorPtr &allocator, size_t size) {
        std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
        allocator.reset(new matchdoc::MatchDocAllocator(pool));
        return allocator->batchAllocate(size);
    }

    std::vector<matchdoc::MatchDoc> createMatchDocsUseSub(matchdoc::MatchDocAllocatorPtr &allocator, size_t size) {
        std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
        allocator.reset(new matchdoc::MatchDocAllocator(pool, true));
        return allocator->batchAllocate(size);
    }

    template <typename T>
    void extendMatchDocAllocator(matchdoc::MatchDocAllocatorPtr allocator,
                                 std::vector<matchdoc::MatchDoc> docs,
                                 const std::string &fieldName,
                                 const std::vector<T> &values) {
        auto pRef = allocator->declare<T>(fieldName, SL_ATTRIBUTE);
        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto value = values[i];
            pRef->set(docs[i], value);
        }
    }

    void extendMatchDocAllocator(matchdoc::MatchDocAllocatorPtr allocator,
                                 std::vector<matchdoc::MatchDoc> docs,
                                 const std::string &fieldName,
                                 const std::vector<std::string> &values) {
        auto pRef = allocator->declare<autil::MultiChar>(fieldName, SL_ATTRIBUTE);
        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto value = values[i];
            auto p = autil::MultiValueCreator::createMultiValueBuffer(value.data(), value.size(), _dataPool.get());
            pRef->getReference(docs[i]).init(p);
        }
    }

    template <typename T>
    void extendMatchDocAllocatorWithConstructFlag(matchdoc::MatchDocAllocatorPtr allocator,
                                                  std::vector<matchdoc::MatchDoc> docs,
                                                  const std::string &fieldName,
                                                  const std::vector<T> &values) {
        auto pRef = allocator->declareWithConstructFlagDefaultGroup<T>(
            fieldName, matchdoc::ConstructTypeTraits<T>::NeedConstruct(), SL_ATTRIBUTE);
        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto value = values[i];
            pRef->set(docs[i], value);
        }
    }

    template <typename T>
    void extendMultiValueMatchDocAllocator(matchdoc::MatchDocAllocatorPtr allocator,
                                           std::vector<matchdoc::MatchDoc> docs,
                                           const std::string &fieldName,
                                           const std::vector<std::vector<T>> &values) {
        typedef typename ColumnDataTypeTraits<T>::value_type ColumnDataType;
        auto pRef = allocator->declare<autil::MultiValueType<ColumnDataType>>(fieldName, SL_ATTRIBUTE);
        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto p = autil::MultiValueCreator::createMultiValueBuffer(values[i], _dataPool.get());
            pRef->getReference(docs[i]).init(p);
        }
    }

    template <typename T>
    void extendMultiValueMatchDocAllocatorWithConstructFlag(matchdoc::MatchDocAllocatorPtr allocator,
                                                            std::vector<matchdoc::MatchDoc> docs,
                                                            const std::string &fieldName,
                                                            const std::vector<std::vector<T>> &values) {
        typedef typename ColumnDataTypeTraits<T>::value_type ColumnDataType;
        auto pRef = allocator->declareWithConstructFlagDefaultGroup<autil::MultiValueType<ColumnDataType>>(
            fieldName,
            matchdoc::ConstructTypeTraits<autil::MultiValueType<ColumnDataType>>::NeedConstruct(),
            SL_ATTRIBUTE);

        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto p = autil::MultiValueCreator::createMultiValueBuffer(values[i], _dataPool.get());
            pRef->getReference(docs[i]).init(p);
        }
    }

private:
    std::shared_ptr<autil::mem_pool::Pool> _dataPool;
};

template <>
inline void MatchDocUtil::extendMatchDocAllocatorWithConstructFlag(matchdoc::MatchDocAllocatorPtr allocator,
                                                                   std::vector<matchdoc::MatchDoc> docs,
                                                                   const std::string &fieldName,
                                                                   const std::vector<std::string> &values) {
    auto pRef = allocator->declareWithConstructFlagDefaultGroup<autil::MultiChar>(
        fieldName, matchdoc::ConstructTypeTraits<autil::MultiChar>::NeedConstruct(), SL_ATTRIBUTE);
    allocator->extend();
    ASSERT_EQ(values.size(), docs.size());
    for (size_t i = 0; i < values.size(); ++i) {
        auto value = values[i];
        auto p = autil::MultiValueCreator::createMultiValueBuffer(value.data(), value.size(), _dataPool.get());
        pRef->getReference(docs[i]).init(p);
    }
}

} // namespace table
