#ifndef __INDEXLIB_PACKAGEFILEFLUSHOPERATIONTEST_H
#define __INDEXLIB_PACKAGEFILEFLUSHOPERATIONTEST_H

#include <autil/StringUtil.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/package_file_flush_operation.h"

IE_NAMESPACE_BEGIN(file_system);

class PackageFileFlushOperationTest : public INDEXLIB_TESTBASE
{
public:
    PackageFileFlushOperationTest();
    ~PackageFileFlushOperationTest();

    DECLARE_CLASS_NAME(PackageFileFlushOperationTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestExecute();
    void TestExecuteWithFlushCache();
						  
private:
    FileNodePtr CreateFileNode(const std::string& path, const std::string& content);
    
    std::string MakeInfoStr(const std::string&path,
                            const std::string& content,
                            bool isAtomicDump = false,
                            bool isCopyOnDump = false,
			    const std::string sepStr="")
    { return path + ":" +
             content + ":" +
             (isAtomicDump ? "true" : "false") + ":" + 
             (isCopyOnDump ? "true" : "false") + 
             sepStr;
    }

    std::string MakeMetaStr(const std::string& path, size_t offset,
                            size_t length, bool isDir,
                            const std::string& sepStr="")
    {
        std::string str = path + ":" + autil::StringUtil::toString(offset)
                     + ":" + autil::StringUtil::toString(length);

        if (isDir)
        {
            str += ":true";
        }
        else
        {
            str += ":false";
        }
        return str + sepStr;
    }

    void CheckDataFile(const std::string& packageFilePath,
		       int dataFileID,
		       const std::string& metaDespStr,
		       size_t expectedPackageFileSize,
		       const std::vector<std::string>& expectedContents);

    void CheckMetaFile(const std::string& packageFilePath,
		       const std::string& metaDespStr);
    
    void InnerTestExecute(
            const std::string& fileNodeInfoStr,
            const std::string& packageFilePath,
            bool hasException,
            size_t expectedPackageFileSize,
            int64_t expectedFlushMemoryUse,
            const std::string& metaDespStr="",
            const PathMetaContainerPtr& flushCache=PathMetaContainerPtr());

private:
    util::BlockMemoryQuotaControllerPtr mMemoryController;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageFileFlushOperationTest, TestExecute);
INDEXLIB_UNIT_TEST_CASE(PackageFileFlushOperationTest, TestExecuteWithFlushCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGEFILEFLUSHOPERATIONTEST_H
