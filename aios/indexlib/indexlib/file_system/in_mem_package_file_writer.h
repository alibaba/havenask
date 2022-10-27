#ifndef __INDEXLIB_IN_MEM_PACKAGE_FILE_WRITER_H
#define __INDEXLIB_IN_MEM_PACKAGE_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/package_file_writer.h"
#include "indexlib/file_system/in_mem_storage.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemPackageFileWriter : public PackageFileWriter
{
public:
    InMemPackageFileWriter(const std::string& filePath,
                           InMemStorage* inMemStorage,
                           const util::BlockMemoryQuotaControllerPtr& mMemController);
                           
    ~InMemPackageFileWriter();

public:
    FileWriterPtr CreateInnerFileWriter(
        const std::string& filePath,
        const FSWriterParam& param = FSWriterParam()) override;

    void MakeInnerDir(const std::string& dirPath) override;

    void GetPhysicalFiles(
            std::vector<std::string>& filePaths) const override;

    void StoreFile(const FileNodePtr& fileNode);
    
    void StoreInMemoryFile(const InMemFileNodePtr& inMemFileNode,
                           const FSDumpParam& dumpParam);
    
public:
    // for test
    static PackageFileMetaPtr GeneratePackageFileMeta(
            const std::string& packageFilePath,
            const std::set<std::string>& innerDirs,
            const std::vector<FileNodePtr>& innerFiles);

private:
    void CollectInnerFileAndDumpParam(std::vector<FileNodePtr>& fileNodeVec,
            std::vector<FSDumpParam>& dumpParamVec);

    void GetDumpParamForFileNodes(const std::vector<FileNodePtr>& fileNodeVec,
                                  std::vector<FSDumpParam>& dumpParamVec);

protected:
    void DoClose() override;
    void AddDirIfNotExist(const std::string& absDirPath);

private:
    typedef std::map<std::string, FileWriterPtr> InnerFileWriterMap;
    typedef std::set<std::string> InnerDirSet;
    typedef std::pair<InMemFileNodePtr, FSDumpParam> InnerInMemoryFile;
    typedef std::map<std::string, InnerInMemoryFile> InnerInMemFileMap;
    
private:
    InnerDirSet mInnerDirs;
    InnerInMemFileMap mInnerInMemFileMap;
    InnerFileWriterMap mInnerFileWriters;
    std::vector<FileNodePtr> mInnerFileNodes;
    
    InMemStorage* mInMemStorage;
    util::BlockMemoryQuotaControllerPtr mMemController;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemPackageFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_IN_MEM_PACKAGE_FILE_WRITER_H
