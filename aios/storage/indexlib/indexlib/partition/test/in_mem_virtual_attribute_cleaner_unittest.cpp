#include "indexlib/partition/test/in_mem_virtual_attribute_cleaner_unittest.h"

#include "indexlib/test/partition_data_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemVirtualAttributeCleanerTest);

InMemVirtualAttributeCleanerTest::InMemVirtualAttributeCleanerTest() {}

InMemVirtualAttributeCleanerTest::~InMemVirtualAttributeCleanerTest() {}

void InMemVirtualAttributeCleanerTest::CaseSetUp() {}

void InMemVirtualAttributeCleanerTest::CaseTearDown() {}

void InMemVirtualAttributeCleanerTest::TestSimpleProcess()
{
    // DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    // PartitionDataMaker::MakePartitionDataFiles(
    //         0, 0, directory, "0,5,5;1,6,6");
    // PartitionDataMaker::MakePartitionDataFiles(
    //         1, 0, directory, "2,5,5;3,6,6");
    // MakeAttributeDir(directory, "segment_0_level_0", "price", false);
    // MakeAttributeDir(directory, "segment_0_level_0", "virtual_attribute", true);
    // MakeAttributeDir(directory, "segment_1_level_0", "price", false);
    // MakeAttributeDir(directory, "segment_2_level_0", "price", false);
    // MakeAttributeDir(directory, "segment_2_level_0", "virtual_attribute", true);

    // ReaderContainerPtr container(new ReaderContainer);
    // InMemVirtualAttributeCleaner cleaner(container, "virtual_attribute", directory);
    // cleaner.Execute();

    // ASSERT_FALSE(directory->IsExist("segment_0_level_0/attribute/virtual_attribute"));
    // ASSERT_TRUE(directory->IsExist("segment_0_level_0/attribute/price"));
    // ASSERT_TRUE(directory->IsExist("segment_1_level_0/attribute/price"));
    // ASSERT_TRUE(directory->IsExist("segment_2_level_0/attribute/price"));
    // ASSERT_TRUE(directory->IsExist("segment_2_level_0/attribute/virtual_attribute"));
}

void InMemVirtualAttributeCleanerTest::MakeAttributeDir(const DirectoryPtr& directory, const string& segName,
                                                        const string& attrName, bool isVirtual)
{
    DirectoryPtr attrDirectory = directory->GetDirectory(segName, true)->MakeDirectory("attribute");
    if (isVirtual) {
        attrDirectory->MakeDirectory(attrName, DirectoryOption::Mem());
    } else {
        attrDirectory->MakeDirectory(attrName);
    }
}
}} // namespace indexlib::partition
