# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import logging
from os import environ

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from .genthrift.hive_metastore import ThriftHiveMetastore
from .genthrift.hive_metastore.ttypes import Database, Table, FieldSchema, Partition, \
    DropPartitionsRequest, RequestPartsSpec

from .genthrift.hive_metastore.ttypes import NoSuchObjectException

SIMPLE_SERDE = 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
INPUT_FORMAT = 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
DEFAULT_PORT = 9083


class HMSClient(ThriftHiveMetastore.Client):
    __client = None
    __isOpened = False

    def __init__(self, iprot=None, oprot=None, host=None, port=None):
        self.logger = logging.getLogger(__name__)

        if not iprot:
            if not host:
                host = environ.get("HMS_HOST")

            if not host:
                host = 'localhost'

            if ':' in host:
                parts = host.split(':')
                host = parts[0]
                port = int(parts[1])

            if not port:
                port = environ.get("HMS_PORT")

            if not port:
                port = DEFAULT_PORT

            transport = TTransport.TBufferedTransport(TSocket.TSocket(host, int(port)))
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            super(HMSClient, self).__init__(protocol, oprot)
        else:
            super(HMSClient, self).__init__(iprot, oprot)

    def open(self):
        self._oprot.trans.open()
        self.__isOpened = True
        return self

    def __enter__(self):
        self.open()
        return self

    def close(self):
        self._oprot.trans.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def make_schema(params):
        """
        Produce field schema from list of parameters

        :param params: list of parameters or tuples
        :type params: list[str]
        :return: resulting field schema
        :rtype: list[FieldSchema]
        """
        schema = []
        for param in params:
            param_type = 'string'
            if ':' in param:
                parts = param.split(':')
                param_name = parts[0]
                param_type = parts[1] if parts[1] else 'string'
            else:
                param_name = param

            schema.append(FieldSchema(name=param_name, type=param_type, comment=''))

        return schema

    @staticmethod
    def parse_schema(schemas):
        """
        Convert list of FieldSchema objects in a list of name:typ strings

        :param schemas:
        :type schemas: list[FieldSchema]
        :return:
        """
        return map(lambda s: '{}\t{}'.format(s.name, s.type), schemas)


    @staticmethod
    def make_partition(table, values):
        """

        :param table:
        :type table: Table
        :param values:
        :type values: list[str]
        :return:
        :rtype: Partition
        """
        partition_names = [k.name for k in table.partitionKeys]
        if len(partition_names) != len(values):
            raise ValueError('Partition values do not match table schema')
        kv = [partition_names[i] + '=' + values[i] for i in range(len(partition_names))]

        sd = copy.deepcopy(table.sd)
        sd.location = sd.location + '/' + '/'.join(kv)

        return Partition(values=values, dbName=table.dbName, tableName=table.tableName, sd=sd)

    def add_partition(self, table, values):
        """
        Add partition

        :param table:
        :type table: Table
        :param values:
        :type values: list[str]
        """
        self.__client.add_partition(self.make_partition(table, values))

    def check_for_named_partition(self, db_name, table_name, partition):
        try:
            self.get_partition_by_name(db_name, table_name, partition)
            return True
        except NoSuchObjectException:
            return False

    def drop_partitions(self, db_name, table_name, names, need_result=None):
        """
        Drop specified partitions from the table

        :param db_name: Database name
        :type db_name: str
        :param table_name:
        :type table_name: str
        :param names: Partition names
        :type names: list[str]
        :param need_result: If true, return drop results
        :return: drop results
        """
        if not names:
            return None
        return self.drop_partitions_req(DropPartitionsRequest(db_name, table_name,
                                                                       RequestPartsSpec(names), need_result))

    def drop_all_partitions(self, db_name, table_name, need_result=None):
        return self.drop_partitions(db_name, table_name,
                                    self.get_partition_names(db_name, table_name),
                                    need_result)

    def get_current_notification_id(self):
        return self.get_current_notificationEventId().eventId

