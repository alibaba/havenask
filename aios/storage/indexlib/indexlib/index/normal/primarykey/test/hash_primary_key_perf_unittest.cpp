#include "indexlib/index/normal/primarykey/test/hash_primary_key_perf_unittest.h"

using namespace std;
using namespace indexlib::test;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HashPrimaryKeyPerfTest);

HashPrimaryKeyPerfTest::HashPrimaryKeyPerfTest() {}

HashPrimaryKeyPerfTest::~HashPrimaryKeyPerfTest() {}

void HashPrimaryKeyPerfTest::CaseSetUp()
{
    CreateHashTable();

    FormatterPtr formatter(new Formatter());
    mFormatter = formatter.get();
    mFormatters.push_back(formatter);
}

void HashPrimaryKeyPerfTest::CaseTearDown() {}

void HashPrimaryKeyPerfTest::TestSimpleProcess() { DoMultiThreadTest(10, 30); }

void HashPrimaryKeyPerfTest::DoWrite()
{
    while (!IsFinished()) {
        FormatterPtr formatter(new Formatter());
        if (!formatter) {
            continue;
            IE_LOG(ERROR, "new Formatter failed");
        }
        mFormatters.push_back(formatter);
        MEMORY_BARRIER();
        mFormatter = formatter.get();
        usleep(1000);
    }
}

void HashPrimaryKeyPerfTest::DoRead(int* status)
{
    while (!IsFinished()) {
        mFormatter->Find(&mData[0], (uint64_t)31);
    }
}

void HashPrimaryKeyPerfTest::CreateHashTable()
{
    mData.resize(1024);

    PrimaryKeyHashTable<uint64_t> hashTable;
    hashTable.Init(&mData[0], 2, 2);

    PKPair<uint64_t> pkPair;
    pkPair.key = 31;
    pkPair.docid = 0;
    hashTable.Insert(pkPair);
    pkPair.key = 131;
    pkPair.docid = 1;
    hashTable.Insert(pkPair);
}
}} // namespace indexlib::index
