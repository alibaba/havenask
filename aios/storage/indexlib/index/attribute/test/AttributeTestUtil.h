#pragma once

#include <memory>

#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"

namespace indexlib::file_system {
class IFileSystem;
}

namespace indexlibv2::index {

class AttributeTestUtil
{
public:
    AttributeTestUtil() = default;
    ~AttributeTestUtil() = default;

public:
    template <typename T>
    static std::shared_ptr<AttributeConfig> CreateAttrConfig(bool isMultiValue, std::optional<std::string> compressType)
    {
        return CreateAttrConfig(TypeInfo<T>::GetFieldType(), isMultiValue, "attr_name", 0, compressType,
                                /*supportNull*/ false,
                                /*updatable*/ false, 0xFFFFFFFFL);
    }

    template <FieldType T>
    static std::shared_ptr<AttributeConfig> CreateAttrConfig(bool isMultiValue, std::optional<std::string> compressType)
    {
        return CreateAttrConfig(T, isMultiValue, "attr_name", 0, compressType,
                                /*supportNull*/ false,
                                /*updatable*/ false, 0xFFFFFFFFL);
    }

    static void CreateDocCounts(uint32_t segmentCount, std::vector<uint32_t>& docCounts);

    static void CreateEmptyDelSets(const std::vector<uint32_t>& docCounts,
                                   std::vector<std::set<docid_t>>& delDocIdSets);

    static void CreateDelSets(const std::vector<uint32_t>& docCounts, std::vector<std::set<docid_t>>& delDocIdSets);

    static std::shared_ptr<AttributeConfig> CreateAttrConfig(FieldType fieldType, bool isMultiValue,
                                                             const std::string& fieldName, fieldid_t fid,
                                                             std::optional<std::string> compressType, bool supportNull,
                                                             bool isUpdatable, uint64_t offsetThreshold);

    static std::shared_ptr<AttributeConfig> CreateAttrConfig(FieldType fieldType, bool isMultiValue,
                                                             const std::string& fieldName, fieldid_t fid,
                                                             const std::string& compressTypeStr, int32_t fixedLen);

    static std::shared_ptr<AttributeConvertor> CreateAttributeConvertor(std::shared_ptr<AttributeConfig> attrConfig);

    static void DumpWriter(AttributeMemIndexer& writer, const std::string& dirPath,
                           const std::shared_ptr<framework::DumpParams>& dumpParams,
                           std::shared_ptr<indexlib::file_system::Directory>& outputDir);

    static void ResetDir(const std::string& dir);
    static void MkDir(const std::string& dir);
    static void CleanDir(const std::string& dir);
    static std::shared_ptr<indexlib::file_system::IFileSystem> CreateFileSystem(const std::string& rootPath);
};

} // namespace indexlibv2::index
