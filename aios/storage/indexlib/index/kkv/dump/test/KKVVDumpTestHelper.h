#pragma once
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/kkv/dump/InlineSKeyDumper.h"
#include "indexlib/index/kkv/dump/KKVDataDumperBase.h"
#include "indexlib/index/kkv/dump/KKVValueDumper.h"
#include "indexlib/index/kkv/dump/NormalSKeyDumper.h"
#include "indexlib/index/kkv/dump/PKeyDumper.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableCreator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace indexlibv2::index {

struct KKVVDumpTestHelper {
    template <typename SKeyType>
    struct SKeyChunkItem : public autil::legacy::Jsonizable {
        bool isLastNode = false;
        SKeyType skey = 0;
        size_t chunkOffset = 0;
        size_t inChunkOffset = 0;
        uint32_t expireTime = 0;
        uint32_t ts = 0;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("isLastNode", isLastNode);
            json.Jsonize("skey", skey, skey);
            json.Jsonize("chunkOffset", chunkOffset, chunkOffset);
            json.Jsonize("inChunkOffset", inChunkOffset, inChunkOffset);
            json.Jsonize("expireTime", expireTime, expireTime);
            json.Jsonize("ts", ts, ts);
        }
    };

    template <typename SKeyType>
    struct SKeyData : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("chunkLength", chunkLength);
            json.Jsonize("chunkItems", chunkItems);
        }
        size_t chunkLength = 0;
        std::vector<SKeyChunkItem<SKeyType>> chunkItems;
    };

    template <typename SKeyType>
    struct InlineSKeyChunkItem : public SKeyChunkItem<SKeyType> {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            SKeyChunkItem<SKeyType>::Jsonize(json);
            json.Jsonize("value", value, value);
        }
        std::string value;
    };

    template <typename SKeyType>
    struct InlineSKeyData : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("chunkLength", chunkLength);
            json.Jsonize("chunkItems", chunkItems);
        }
        size_t chunkLength = 0;
        std::vector<InlineSKeyChunkItem<SKeyType>> chunkItems;
    };

    // R"(
    //      [
    //       {
    //         "chunkLength": 4,
    //         "chunkItems": ["1", "0"]
    //       }
    //      ]
    //     )";
    static void CheckValue(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                           const std::string& expectedValueFileData);

    // R"(
    //      [
    //       {
    //         "chunkLength": 28,
    //         "chunkItems":
    //         [
    //          {"isLastNode":false,"skey":1,"chunkOffset":0,"inChunkOffset":0},
    //          {"isLastNode":true,"skey":0,"chunkOffset":0,"inChunkOffset":2},
    //         ]
    //       }
    //      ]
    //     )";
    template <typename SKeyType>
    static void CheckSKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, bool storeTs,
                          bool storeExpireTime, const std::string& expectedSkeyFileData);

    // R"(
    //      [
    //       {
    //         "chunkLength": 28,
    //         "chunkItems":
    //         [
    //          {"isLastNode":false,"skey":1,"value":"1"},
    //          {"isLastNode":true,"skey":0,"value":"0"},
    //         ]
    //       }
    //      ]
    //     )";
    template <typename SKeyType>
    static void CheckInlineSKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, bool storeTs,
                                bool storeExpireTime, const std::string& expectedSkeyFileData);

    // R"(
    //      [
    //       {
    //         "pkey":1,
    //         "chunkOffset": 28,
    //         "inChunkOffset": 0,
    //         "blockHint": 0,
    //       }
    //      ]
    //     )";
    static void CheckPKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                          const indexlib::config::KKVIndexPreference& indexPreference,
                          const std::string& expectedPkeyFileData);

    // format: "cmd=add,pkey=1,skey=2,value=3,ts=4,expire_time=5"
    static void ParseKKVMessage(const std::string& kkvDocStr, uint64_t& pkey, bool& isDeletedPkey, bool isDeletedSkey,
                                uint64_t& skey, uint32_t& timestamp, uint32_t& expireTime, string& value);

    static void Dump(KKVDataDumperBase& dumper, const std::string& message, bool isLastSkey,
                     autil::mem_pool::Pool& pool);

private:
    AUTIL_LOG_DECLARE();
};

template <typename SKeyType>
void KKVVDumpTestHelper::CheckSKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, bool storeTs,
                                   bool storeExpireTime, const std::string& expectedSkeyFileData)
{
    std::vector<SKeyData<SKeyType>> expectedChunks;
    autil::legacy::FromJsonString(expectedChunks, expectedSkeyFileData);
    ASSERT_FALSE(expectedChunks.empty());

    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = directory->CreateFileReader("skey", readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    for (size_t i = 0; i < expectedChunks.size(); ++i) {
        ChunkMeta chunkMeta;
        ASSERT_TRUE(fileReader->Read(&(chunkMeta), sizeof(chunkMeta)).OK());
        ASSERT_EQ(expectedChunks[i].chunkLength, chunkMeta.length) << i;

        for (const auto& expectedItem : expectedChunks[i].chunkItems) {
            NormalOnDiskSKeyNode<SKeyType> skeyNode;
            ASSERT_TRUE(fileReader->Read(&(skeyNode), sizeof(skeyNode)).OK());
            ASSERT_EQ(expectedItem.skey, skeyNode.skey) << i;
            ASSERT_EQ(expectedItem.isLastNode, skeyNode.IsLastNode()) << i;
            ASSERT_EQ(expectedItem.chunkOffset, skeyNode.GetValueOffset().chunkOffset) << i;
            ASSERT_EQ(expectedItem.inChunkOffset, skeyNode.GetValueOffset().inChunkOffset) << i;

            if (storeTs) {
                int32_t ts = 0;
                ASSERT_TRUE(fileReader->Read(&(ts), sizeof(ts)).OK()) << i;
                ASSERT_EQ(expectedItem.ts, ts) << i;
            }
            if (storeExpireTime) {
                int32_t expireTime = 0;
                ASSERT_TRUE(fileReader->Read(&(expireTime), sizeof(expireTime)).OK()) << i;
                ASSERT_EQ(expectedItem.expireTime, expireTime) << i;
            }
        }
    }
    ASSERT_EQ(fileReader->Tell(), fileReader->GetLogicLength()) << fileReader->GetLogicLength();
}

template <typename SKeyType>
void KKVVDumpTestHelper::CheckInlineSKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                         bool storeTs, bool storeExpireTime, const std::string& expectedSkeyFileData)
{
    std::vector<InlineSKeyData<SKeyType>> expectedChunks;
    autil::legacy::FromJsonString(expectedChunks, expectedSkeyFileData);
    ASSERT_FALSE(expectedChunks.empty());

    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_MMAP);
    readerOption.isSwapMmapFile = false;
    auto [status, fileReader] = directory->CreateFileReader("skey", readerOption).StatusWith();
    ASSERT_TRUE(status.IsOK());

    for (size_t i = 0; i < expectedChunks.size(); ++i) {
        ChunkMeta chunkMeta;
        ASSERT_TRUE(fileReader->Read(&(chunkMeta), sizeof(chunkMeta)).OK());
        ASSERT_EQ(expectedChunks[i].chunkLength, chunkMeta.length) << i;

        for (const auto& expectedItem : expectedChunks[i].chunkItems) {
            InlineOnDiskSKeyNode<SKeyType> skeyNode;
            ASSERT_TRUE(fileReader->Read(&(skeyNode), sizeof(skeyNode)).OK());
            ASSERT_EQ(expectedItem.skey, skeyNode.skey) << i;
            ASSERT_EQ(expectedItem.isLastNode, skeyNode.IsLastNode()) << i;

            if (storeTs) {
                int32_t ts = 0;
                ASSERT_TRUE(fileReader->Read(&(ts), sizeof(ts)).OK()) << i;
                ASSERT_EQ(expectedItem.ts, ts) << i;
            }
            if (storeExpireTime) {
                int32_t expireTime = 0;
                ASSERT_TRUE(fileReader->Read(&(expireTime), sizeof(expireTime)).OK()) << i;
                ASSERT_EQ(expectedItem.expireTime, expireTime) << i;
            }

            ASSERT_EQ(skeyNode.IsValidNode(), !expectedItem.value.empty()) << i;
            ASSERT_TRUE(expectedItem.value.size() < 64) << "tested value size should be smaller than 64";
            if (skeyNode.IsValidNode()) {
                int8_t size = 0;
                ASSERT_TRUE(fileReader->Read(&(size), sizeof(size)).OK()) << i;
                ASSERT_TRUE(size < 64);
                string data(size, '\0');
                ASSERT_TRUE(fileReader->Read(data.data(), size).OK()) << i;
                ASSERT_EQ(data, expectedItem.value);
            }
        }
    }
    ASSERT_EQ(fileReader->Tell(), fileReader->GetLogicLength()) << fileReader->GetLogicLength();
}

} // namespace indexlibv2::index
