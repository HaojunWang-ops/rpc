#include "Exception.h"
#include "CurrentThread.h"

namespace reactor{
    Exception::Exception(string message)
        : message_(std::move(message)),
          stack_(CurrentThread::stackTrace(1))
    {}
}