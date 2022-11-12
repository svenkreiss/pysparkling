#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = ["StorageLevel"]


class StorageLevel:

    """
    Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
    whether to drop the RDD to disk if it falls out of memory, whether to keep the data in memory
    in a JAVA-specific serialized format, and whether to replicate the RDD partitions on multiple
    nodes. Also contains static constants for some commonly used storage levels, MEMORY_ONLY.
    Since the data is always serialized on the Python side, all the constants use the serialized
    formats.
    """

    def __init__(self, useDisk, useMemory, useOffHeap, deserialized, replication=1):
        self.useDisk = useDisk
        self.useMemory = useMemory
        self.useOffHeap = useOffHeap
        self.deserialized = deserialized
        self.replication = replication

    def __repr__(self):
        return (
            f"StorageLevel({self.useDisk}, {self.useMemory}, {self.useOffHeap}, {self.deserialized}, "
            f"{self.replication})"
        )

    def __str__(self):
        result = ""
        result += "Disk " if self.useDisk else ""
        result += "Memory " if self.useMemory else ""
        result += "OffHeap " if self.useOffHeap else ""
        result += "Deserialized " if self.deserialized else "Serialized "
        result += f"{self.replication}x Replicated"
        return result


StorageLevel.DISK_ONLY = StorageLevel(True, False, False, False)
StorageLevel.DISK_ONLY_2 = StorageLevel(True, False, False, False, 2)
StorageLevel.MEMORY_ONLY = StorageLevel(False, True, False, False)
StorageLevel.MEMORY_ONLY_2 = StorageLevel(False, True, False, False, 2)
StorageLevel.MEMORY_AND_DISK = StorageLevel(True, True, False, False)
StorageLevel.MEMORY_AND_DISK_2 = StorageLevel(True, True, False, False, 2)
StorageLevel.OFF_HEAP = StorageLevel(True, True, True, False, 1)
