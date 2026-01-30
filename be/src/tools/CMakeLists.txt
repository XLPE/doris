# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/tools")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/tools")

add_executable(meta_tool
    meta_tool.cpp
)

pch_reuse(meta_tool)

# This permits libraries loaded by dlopen to link to the symbols in the program.
set_target_properties(meta_tool PROPERTIES ENABLE_EXPORTS 1)

target_link_libraries(meta_tool
    ${DORIS_LINK_LIBS}
)

install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
install(TARGETS meta_tool DESTINATION ${OUTPUT_DIR}/lib/)

if (NOT OS_MACOSX)
# Meta tool never need debug info
add_custom_command(TARGET meta_tool POST_BUILD
    COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:meta_tool> $<TARGET_FILE:meta_tool>.dbg
    COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:meta_tool>
    COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:meta_tool>.dbg $<TARGET_FILE:meta_tool>
    )
endif()



# 添加rowset_debug可执行目标
add_executable(rowset_debug
    rowset_debug.cpp
)

# 复用预编译头
pch_reuse(rowset_debug)

# 设置与meta_tool相同的属性（允许dlopen加载的库链接到程序符号）
set_target_properties(rowset_debug PROPERTIES ENABLE_EXPORTS 1)

# 链接Doris库
target_link_libraries(rowset_debug
    ${DORIS_LINK_LIBS}
)

# 添加到安装目标
install(TARGETS rowset_debug DESTINATION ${OUTPUT_DIR}/lib/)

# 非macOS系统的调试信息处理（与meta_tool保持一致）
if (NOT OS_MACOSX)
add_custom_command(TARGET rowset_debug POST_BUILD
    COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:rowset_debug> $<TARGET_FILE:rowset_debug>.dbg
    COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:rowset_debug>
    COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:rowset_debug>.dbg $<TARGET_FILE:rowset_debug>
    )
endif()
