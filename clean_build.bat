@echo off
REM 停止可能正在运行的程序实例
taskkill /F /IM wesql.exe 2>nul

REM 彻底清理构建目录
if exist build (rd /s /q build)

REM 创建新的构建目录并构建
mkdir build
cd build
cmake -G "MinGW Makefiles" -DCMAKE_C_COMPILER="D:/msys2/mingw64/bin/gcc.exe" -DCMAKE_CXX_COMPILER="D:/msys2/mingw64/bin/g++.exe" ..
cmake --build . --config Release

REM 返回上级目录
cd ..

REM 运行程序
build\wesql.exe