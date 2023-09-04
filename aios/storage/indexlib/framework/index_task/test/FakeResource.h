#pragma once

#include <string>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"

namespace indexlibv2::framework {

class FakeVectorResource : public IndexTaskResource
{
public:
    FakeVectorResource() : IndexTaskResource(/*name*/ "test", /*type*/ "vector") {}

    const std::vector<int>& GetData() const { return _data; }
    void SetData(std::vector<int> data) { _data = std::move(data); }

    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override
    {
        auto fileWriter = resourceDirectory->CreateFileWriter("data");
        assert(fileWriter);
        size_t sz = _data.size();
        fileWriter->Write(&sz, sizeof(sz)).GetOrThrow();
        for (auto x : _data) {
            fileWriter->Write(&x, sizeof(x)).GetOrThrow();
        }
        fileWriter->Close().GetOrThrow();
        return Status::OK();
    }

    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override
    {
        auto fileReader = resourceDirectory->CreateFileReader(
            "data", indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_BUFFERED));
        assert(fileReader);
        _data.clear();
        size_t sz = 0;
        if (auto st = fileReader->Read(&sz, sizeof(sz)).Status(); !st.IsOK()) {
            return st;
        }
        for (size_t i = 0; i < sz; ++i) {
            int x = 0;
            if (auto st = fileReader->Read(&x, sizeof(x)).Status(); !st.IsOK()) {
                return st;
            }
            _data.push_back(x);
        }
        return Status::OK();
    }

private:
    std::vector<int> _data;
};

class FakeResourceCreator : public IIndexTaskResourceCreator
{
public:
    std::unique_ptr<IndexTaskResource> CreateResource(const std::string& name,
                                                      const IndexTaskResourceType& type) override
    {
        if (type == "vector") {
            return std::make_unique<FakeVectorResource>();
        } else {
            return nullptr;
        }
    }
};

} // namespace indexlibv2::framework
