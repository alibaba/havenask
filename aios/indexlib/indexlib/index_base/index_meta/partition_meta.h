#ifndef __INDEXLIB_PARTITION_META_H
#define __INDEXLIB_PARTITION_META_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/sort_description.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

typedef common::SortDescription SortDescription;
typedef common::SortDescriptions SortDescriptions;

class PartitionMeta : public autil::legacy::Jsonizable
{
public:
    PartitionMeta();
    PartitionMeta(const common::SortDescriptions &sortDescs);
    ~PartitionMeta();
public:
    const SortDescription& GetSortDescription(size_t idx) const;
    void AddSortDescription(const std::string &sortField,
                            const SortPattern &sortPattern);
    void AddSortDescription(const std::string &sortField,
                            const std::string &sortPatternStr);

    SortPattern GetSortPattern(const std::string& sortField);

    void AssertEqual(const PartitionMeta &other);

    bool HasSortField(const std::string& sortField) const;

    const common::SortDescriptions& GetSortDescriptions() const { return mSortDescriptions; }

    size_t Size() const { return mSortDescriptions.size(); }
public:
    void Store(const std::string &rootDir) const;
    void Store(const file_system::DirectoryPtr& directory) const;
    void Load(const std::string &rootDir);
    void Load(const file_system::DirectoryPtr& directory);
    static bool IsExist(const std::string &rootDir);
    static PartitionMeta LoadPartitionMeta(const std::string &rootDir);
    static PartitionMeta LoadFromString(const std::string& jsonStr);
public:
    void Jsonize(JsonWrapper& json) override;
private:
    common::SortDescriptions mSortDescriptions;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionMeta);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARTITION_META_H
