#ifndef __INDEXLIB_INDEX_FORMAT_VERSION_H
#define __INDEXLIB_INDEX_FORMAT_VERSION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

class IndexFormatVersion: public autil::legacy::Jsonizable
{
public:
    IndexFormatVersion(const std::string& version = std::string());
    ~IndexFormatVersion();

public:
    void Set(const std::string& version);

    bool Load(const std::string& path, bool mayNonExist = false);
    bool Load(const file_system::DirectoryPtr& directory, bool mayNonExist = false);
    void Store(const std::string& path);
    void Store(const file_system::DirectoryPtr& directory);

    bool operator == (const IndexFormatVersion& other) const;
    bool operator != (const IndexFormatVersion& other) const;

    void CheckCompatible(const std::string& binaryVersion);
    bool IsLegacyFormat() const;

    static IndexFormatVersion LoadFromString(const std::string& content);
    static void StoreBinaryVersion(const std::string& rootDir);

    bool operator < (const IndexFormatVersion& other) const;
    bool operator > (const IndexFormatVersion& other) const
    {
        return *this == other ? false : !(*this < other);
    }
    
    bool operator <= (const IndexFormatVersion& other) const
    {
        return *this < other || *this == other;
    }
    
    bool operator >= (const IndexFormatVersion& other) const
    {
        return !(*this < other);
    }
    
protected:
    void Jsonize(JsonWrapper& json) override;
private:
    void SplitVersionIds(const std::string& versionStr, 
                         std::vector<int32_t>& versionIds) const;

private:
    std::string mIndexFormatVersion;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexFormatVersion);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_INDEX_FORMAT_VERSION_H
