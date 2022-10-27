#ifndef __INDEXLIB_TRUNCATE_INDEX_NAME_MAPPER_H
#define __INDEXLIB_TRUNCATE_INDEX_NAME_MAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_schema.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(config);

struct TruncateIndexNameItem : public autil::legacy::Jsonizable
{
public:
    TruncateIndexNameItem();
    TruncateIndexNameItem(const std::string& indexName,
                          const std::vector<std::string>& truncNames);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    std::string mIndexName;
    std::vector<std::string> mTruncateNames;
};
DEFINE_SHARED_PTR(TruncateIndexNameItem);

////////////////////////////////////////////////////////////////////

class TruncateIndexNameMapper : public autil::legacy::Jsonizable
{
public:
    TruncateIndexNameMapper(const std::string& truncateMetaDir);
    ~TruncateIndexNameMapper();

public:
    void Load();
    void Load(const file_system::DirectoryPtr& metaDir);
    void Dump(const IndexSchemaPtr& indexSchema);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool Lookup(const std::string& indexName,
                std::vector<std::string>& truncNames) const;
public:
    //public for test
    void AddItem(const std::string &indexName, 
                 const std::vector<std::string>& truncateIndexNames);
    void DoDump(const std::string& mapperFile);

private:
    TruncateIndexNameItemPtr Lookup(const std::string& indexName) const;

private:
    typedef std::vector<TruncateIndexNameItemPtr> ItemVector;
    typedef std::map<std::string, size_t> NameMap;

private:
    std::string mTruncateMetaDir;
    ItemVector mTruncateNameItems;
    NameMap mNameMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexNameMapper);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_INDEX_NAME_MAPPER_H
