#pragma once

#include <sys/types.h>
#include <string>
#include <unistd.h>

namespace reactor
{
    namespace CurrentThread
    {
        //char[] 和 const char*
        //char[] 自己生产 需要修改 独占内存
        //const char* 借用数据 只读不改 避免拷贝
        extern __thread int t_cachedTid;
        extern __thread char t_tidString[32];    
        extern __thread int t_tidStringLength;
        extern __thread const char *t_threadName;
        void cacheTid();

        inline int tid()
        {
            //long __builtin_expect (long exp, long c);
            //exp实际计算或判断的表达式 c期望表达式的值
            //返回值exp的计算结果
            //exp在大多数情况下等于c

            if (__builtin_expect(t_cachedTid == 0, 0))
            {
                cacheTid();
            }
            return t_cachedTid;
        }

        inline const char* tidString()
        {
            return t_tidString;
        }

        inline int tidStringLength()
        {
            return t_tidStringLength;
        }

        inline const char *name()
        {
            return t_threadName;
        }

        inline bool isMainThread(){return tid() == ::getpid();};


        std::string stackTrace(bool demangle);
    }
}