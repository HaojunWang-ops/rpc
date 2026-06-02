#include "Buffer.h"
#include "SocketsOps.h"
#include "base/Types.h"

#include <errno.h>
#include <sys/uio.h>

using namespace reactor;
using namespace reactor::net;

const size_t Buffer::kCheapPrepend;
const size_t Buffer::kInitialSize;

ssize_t Buffer::readFd(int fd, int *savedErrno)
{
    char extrabuf[65536];//分配在线程栈上的空间
    struct iovec vec[2];
    const size_t writeable = writeableBytes();
    vec[0].iov_base = begin() + writeIndex_;
    vec[0].iov_len = writeable;
    vec[1].iov_base = extrabuf;
    vec[1].iov_len = sizeof extrabuf;

    // 如果buffer足够大，全部写进buffer中
    // buffer < extrabuf的时候，写到两个中
    // 所以最大的容量是128k-1
    const int iovcnt = (writeable < sizeof extrabuf) ? 2 : 1;
    const ssize_t n = sockets::readv(fd, vec, iovcnt);
    if (n < 0)
    {
        *savedErrno = errno;
    }
    else if (implicit_cast<size_t>(n) <= writeable)
    {
        writeIndex_ += n;
    }
    else
    {
        writeIndex_ = buffer_.size();
        append(extrabuf, n - writeable);
    }

    return n;
}
