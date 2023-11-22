#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::util;
namespace indexlib { namespace index {

class PrimaryKeyFormatterHelper
{
public:
    static const string PK_FILE_NAME;
    static const string PK_DIRECTORY_PATH;

    // @outputRoot used for create file system
    static DirectoryPtr PrepareMmapDirectory(bool isLock, string outputRoot);
    static DirectoryPtr PrepareCacheDirectory(const string& cacheType, size_t blockSize, string outputRoot);

    // return the pk file name
    // @data should be sorted
    template <typename Key>
    static string PreparePkData(const vector<PKPair<Key>>& data, PrimaryKeyFormatter<Key>& formatter,
                                DirectoryPtr directory);

private:
    IE_LOG_DECLARE();
};

template <typename Key>
string PrimaryKeyFormatterHelper::PreparePkData(const vector<PKPair<Key>>& data, PrimaryKeyFormatter<Key>& formatter,
                                                DirectoryPtr directory)
{
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    directory->RemoveFile(PK_FILE_NAME, removeOption);
    FileWriterPtr writer = directory->CreateFileWriter(PK_FILE_NAME);
    typename PrimaryKeyFormatter<Key>::HashMapTypePtr hashMap(
        new typename PrimaryKeyFormatter<Key>::HashMapType(data.size()));
    for (auto& pkPair : data) {
        hashMap->FindAndInsert(pkPair.key, pkPair.docid);
    }
    autil::mem_pool::Pool pool;
    formatter.SerializeToFile(hashMap, data.size(), &pool, writer);
    writer->Close().GetOrThrow();
    return PK_FILE_NAME;
}
}} // namespace indexlib::index
