#ifndef ISEARCH_BUFFEREDLOCALFILEREADER_H
#define ISEARCH_BUFFEREDLOCALFILEREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <fstream>
#include <ha3/util/BufferedFileReader.h>
BEGIN_HA3_NAMESPACE(util);

class BufferedLocalFileReader : public BufferedFileReader
{
public:
    BufferedLocalFileReader(const std::string &fileName);
    virtual ~BufferedLocalFileReader();
protected:
    virtual int32_t doRead(void *buffer, int32_t size) ;
    virtual void doSeek(int64_t pos);
    virtual int64_t doGetPosition();
private:
    std::ifstream _fin;
    int64_t _fileLength;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BufferedLocalFileReader);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_BUFFEREDLOCALFILEREADER_H
