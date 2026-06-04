#pragma once

#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <iomanip>
#include <iostream>

inline bool WriteN(int fd, void* buf, size_t n)
{
    char* p = static_cast<char*>(buf);
    size_t left = n;

    while (left > 0)
    {
        int ret = ::write(fd, p, left);
        if (ret > 0)
        {
            left -= ret;
            p += ret;
        }
        else if (ret <= 0)
        {
            if (errno == EAGAIN)
            {
                continue;
            }
            else
            {
                return false;
            }
        }
    }
    return true;
}

inline bool ReadN(int fd, void* buf, size_t n)
{
    char* p = static_cast<char*> (buf);
    size_t left = n;
    
    while(left > 0)
    {
        int ret = ::read(fd, p, left);
        if (ret > 0)
        {
            left -= ret;
            p += ret;
        }
        else if (ret <= 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                std::cerr << "read timeout\n";
                return false;
            }
            
            perror("read");
            return false;
        }
    }
    return true;
}

inline void PrintHex(const std::string& name, const std::string& s)
{
    std::cout << name << " size= " << s.size() << " hex= ";

    for (unsigned char c : s){
        std::cout << std::hex
                  << std::setw(2)
                  << std::setfill('0')
                  << static_cast<int> (c)
                  <<" ";
    }

    std::cout << std::dec << "\n";
}